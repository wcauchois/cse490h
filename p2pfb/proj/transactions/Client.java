package transactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;



import main.AppUtil;
import main.ErrorCode;
import main.RPCNode;

import paxos.Services;
import primitives.Handle;
import primitives.Lock;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.Update;
import proto.RPCProtos.VersionUpdate;
import proto.RPCProtos.Update.Builder;
import tests.CallResultListener;

/**
 * A client has its own write-through cache, runs transactions locally (going to the server on cache misses), and
 * sends compressed commits to the servers.
 * 
 */
public class Client extends TransactionBase {
    private static final String NEXT_TX_ID = "next_tx_id.txt";
    
    private class PendingUpdate extends Object { // <---  aaahahahaha
        @SuppressWarnings("unchecked")
        public Handle resultHandle; // for returning results after the transaction has completed
        public String filename;
        public int version;
        public Update.Type type;
        public String contents; // for a get(), we need to print the results to the screen 
        // after the transaction has been committed successfully
        
        @SuppressWarnings("unchecked")
        public PendingUpdate(Handle resultHandle, String filename, int version, Update.Type mod) {
            this.resultHandle = resultHandle;
            this.filename = filename;
            this.version = version;
            this.type = mod;
        }
        
        @SuppressWarnings("unchecked")
        public PendingUpdate(Handle resultHandle, String filename, int version, Update.Type mod, String contents) {
            this(resultHandle, filename, version, mod);
            this.contents = contents;
        }
        
        @Override
        public String toString() {
            return "PendingUpdate: filename " + filename + " version " + version + " type " + type;
        }
    }
    
    // the address of the server that is /probably/ the Paxos Leader
    private int leaderHint;
    // are we currently processing a transaction?
    private boolean processingTransaction = false;
    // the highest ranked modification for each file we've made within the current transaction
    private Map<String, Update.Type> fileModifications = new HashMap<String, Update.Type>();
    // the ordered log of updates we've made for the current transaction
    private List<PendingUpdate> transactionLog = new  ArrayList<PendingUpdate>();
    // files currently in our cache
    // filename -> version number
    private Map<String, Integer> fileVersions = new HashMap<String, Integer>();
    // We need to  need to make all operations on the client linearized in case the
    // user makes logic decisions based on previous operations
    private Lock lock = new Lock();
    // whether the current transaction did not begin with a txStart()
    private boolean singleOperationTransaction = false;
    // The Id of our current transaction
    private int currentTxId;
    // We keep all transaction IDs unique
    private int nextTxId;
    // We only run one transaction at a time -- this global state makes redirections much easier
    private Handle<Object> commitHandle;
    private List<Update> compressedTxLog;
    
    public Client(RPCNode node, Services services) {
        super(node, services);
        Integer lastId = services.getFileSystem().getID(NEXT_TX_ID);
        nextTxId = (lastId == null) ? node.addr : lastId;
        leaderHint = services.getPaxosAddrs().first();
    }
    
    protected void abort() {
        if(processingTransaction) {
            logError("aborting transaction mmmmk...");
            clearCache(fileModifications.keySet());
            clearTransactionState();
        }
    }
    
    private void clearTransactionState() {
        processingTransaction = false;
        singleOperationTransaction = false;
        for(String badFile : fileModifications.keySet())
            fileVersions.remove(badFile);
        fileModifications.clear();
        transactionLog.clear();
    }
    
    // do we really need to do this? we can easily just leave a bunch of garbage on our FS
    private void clearCache(Collection<String> files) {
        for(String filename : files) {
            try {
                services.getFileSystem().deleteFile(filename);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private void txStart() {
        log("txstart");
        if(processingTransaction) {
            logError("Warning: already processing a tx! flushing old transaction...");
            abort(); // flush out the previous transaction... clients can only run one transaction at a time
        }
        processingTransaction = true;
    }
    
    private void txAbort() {
        log("txabort");
        if(!processingTransaction) {
            logError("received a txabort without a previous txcommit");
            return;
        }
        
        abort();
    }
    
    private Handle<Object> txCommit() {
        commitHandle = new Handle<Object>();
        singleOperationTransaction = false; // hmmm, seems out of place...
        
        if(!processingTransaction) {
            logError("received a txCommit without a previous txstart");
            commitHandle.completedError(ErrorCode.ERR_NONE); // XXX?
            return commitHandle;
        }
        
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                compressedTxLog = compressTxLog();
                
                currentTxId = nextTxId;
                nextTxId += services.getNodeCount(); // XXX: what if the node count is changing?
                // TODO: should the TX ID be a string of the form "addr_id" ? 
                services.getFileSystem().updateID(NEXT_TX_ID, nextTxId);
                
                Handle<CommitResponse> callResult = sendCommitAttempt(leaderHint, currentTxId, compressedTxLog);
                callResult.addListener(recursiveCommitListener());
            }
        });
        
        return commitHandle;
    }
    
    private void onCommitStateChange(int forwardTo) {
        leaderHint = forwardTo;
        
        // The server we sent our RPC to was not the leader, so we need to forward our request
        Handle<CommitResponse> callResult = sendCommitAttempt(leaderHint, currentTxId, compressedTxLog);
        // Holy shit, it's a recursive Listener!
        callResult.addListener(recursiveCommitListener());
    }
    
    @SuppressWarnings("unchecked")
    private void onCommitStateChange(List<VersionUpdate> versionUpdates) {
        for(VersionUpdate update : versionUpdates) {
            fileVersions.put(update.getFilename(), update.getVersion());
            fileModifications.remove(update.getFilename());
        }
        
        // notify all pending calls of success
        for(PendingUpdate pending : transactionLog) {
            pending.resultHandle.completedSuccess(pending.contents);
        }
        
        commitHandle.completedSuccess(null);
        clearTransactionState();
        lock.release();
    }
    
    // in case we need to foward our request to another node
    private Handle.Listener<CommitResponse> recursiveCommitListener() {
        // we construct the Listener as an anonymous class rather than a normal class
        // so that we can take advantage of closures
        return new Handle.Listener<CommitResponse>() {
            @SuppressWarnings("unchecked")
            @Override
            public void onSuccess(CommitResponse response) {
                if(response.hasForwardTo()) {
                    // update our hint for which server is acting as the leader, and retry
                    onCommitStateChange(response.getForwardTo());
                } else {
                    onCommitStateChange(response.getUpdatedVersionsList());
                }
            }
            
            @Override
            public void onError(int err) {
                if(err == ErrorCode.ERR_SERVICE_FAILURE) {
                    // resend transactions to another server
                    onCommitStateChange(updateLeaderHint());
                } else {
                    // notify all pending calls of error   (/which/ file is the error associated with?....)
                    for(PendingUpdate pending : transactionLog) {
                        pending.resultHandle.completedError(err);
                    }
                    handleUnrecoverableError(commitHandle, err);
                }
            }
        };
    }
     
    // rather than sending the complete list of updates to the server, we 
    // compress the transaction log so that the server only needs to process the
    // most recent update to each file
    // precondition: no multiple create()s delete()s for any file
    private List<Update> compressTxLog() {
        // consider the following transaction:
        //  1. create A
        //  2. get B
        //  3. put A
        //  4. get A
        //  5. put B
        //
        // the /data/ that we want to send to the server is from the last write (3, 5).
        // the update /type/ we want to return is the highest in the sequence (1, 5)
        // Does the order of the "threads" (series of modifications to any given file) 
        // matter? We could send this to the server:
        // [[file A, contents from 3, type from 1], 
        //  [file B, contents from 5, type from 5]]
        // 
        // or we could send this:
        // [[file B, contents from 5, type from 5],
        //  [file A, contents from 3, type from 1]]
        //
        // Does it matter? It seems like it shouldn't...
        
        Map<String, String> mostRecentContents = new HashMap<String, String>();
        List<Update> compressedTxLog = new ArrayList<Update>();
        
        List<PendingUpdate> reversedLog = new ArrayList<PendingUpdate>(transactionLog);
        Collections.reverse(reversedLog);
        for(PendingUpdate pendingUpdate : reversedLog) {
            if(pendingUpdate.type == Update.Type.Write &&
                    !mostRecentContents.containsKey(pendingUpdate.filename)) {
                mostRecentContents.put(pendingUpdate.filename, pendingUpdate.contents);
            }
        }
        
        for(String filename : fileModifications.keySet()) {
            Builder update = Update.newBuilder()
                .setFilename(filename)
                .setType(fileModifications.get(filename));
                
            if(fileVersions.containsKey(filename))
                update.setVersion(fileVersions.get(filename));
            
            if(mostRecentContents.containsKey(filename))
                update.setContents(mostRecentContents.get(filename));
                
            compressedTxLog.add(update.build());
        }
        
        log("Compressed transaction log:" + compressedTxLog.toString().replace("\n", " "));
        
        return compressedTxLog;
    }
    
    @Override
    public void onCommand(String command) {
        try {
            String[] args = null;
            if (command.startsWith("txstart")) {
                txStart();
                
            } else if (command.startsWith("txcommit")) {
                txCommit().addListener(new CallResultListener<Object>("txcommit", this));
                
            } else if (command.startsWith("txabort")) {
                txAbort();
                
            } else if (command.startsWith("create ")) { // create filename
                args = AppUtil.splitArgs(command, 2);
                create(args[1]).addListener(new CallResultListener<Object>("create", this));
                
            } else if (command.startsWith("get ")) { // get filename
                args = AppUtil.splitArgs(command, 2);
                // CallResultListener is actually the exact functionality we want out of the get handler, 
                // since it prints the results to the screen
                get(args[1]).addListener(new CallResultListener<String>("get", this));
                
            } else if (command.startsWith("put ")) { // put filename contents
                args = AppUtil.splitArgs(command, 3);
                put(args[1], args[2]).addListener(new CallResultListener<Object>("put", this));
                
            } else if (command.startsWith("append ")) { // append filename contents
                args = AppUtil.splitArgs(command, 3);
                append(args[1], args[2]).addListener(new CallResultListener<Object>("append", this));
                
            } else if (command.startsWith("delete ")) { // delete filename
                args = AppUtil.splitArgs(command, 2);
                delete(args[1]).addListener(new CallResultListener<Object>("delete", this));
                
            } else if(command.startsWith("info")) { // info
                dumpInfo();
            } else {
                rpcNode.logError("unkown command: " + command);
            }
        } catch(IllegalArgumentException e) {
            // This also catches NumberFormatException from Integer.parseInt()
            logError("bad command syntax");
        }
    }
    
    // We use a crazy state machine to update our modification level:
    // 
    //  return: the error code of a failed modification, or zero if the modification change was successful
    // TODO: this is far too C-like -- throw an exception? That would require defining a new exception type...
    private int updateModificationLevel(String filename, Update.Type newMod) {
        if(!fileModifications.containsKey(filename)) {
            fileModifications.put(filename, newMod);
            return 0;
        } else {
            Update.Type oldMod = fileModifications.get(filename);
            switch(oldMod) {
            case Read: 
                if(newMod == Update.Type.Create) {
                    return ErrorCode.ERR_FILE_EXISTS;
                } else {
                    fileModifications.put(filename, newMod); // R->W, R->R, or R->D
                }
                break;
            case Write:
                if(newMod == Update.Type.Create) {
                    return ErrorCode.ERR_FILE_EXISTS;
                } else if(newMod == Update.Type.Delete) {
                    fileModifications.put(filename, newMod);
                } // else if R, W->W, else if W, W->W
                break;
            case Create: 
                if(newMod == Update.Type.Create) {
                    return ErrorCode.ERR_FILE_EXISTS;
                } else if(newMod == Update.Type.Delete) { // C->D, remove ourselves from the the log entirely (temp file)
                    fileModifications.remove(filename);
                    purgeFileFromLog(filename);
                    return -1; // XXX HACK!!! We need to the caller signal not to append the delete after I've purged the log. 
                } // else if R, C->C, else if W, C->C
                break;
            case Delete:
                if(newMod == Update.Type.Create) {
                    logError("Creates following deletes are disallowed... if you want to empty a file, call put(\"\")");
                    return ErrorCode.ERR_ILLEGAL_OPERATION;
                } else { // R, W, D, -> no such file
                    return ErrorCode.ERR_FILE_NOT_FOUND;
                }
            }
        }
        return 0;
    }
    
    // called on a Delete followed by a Create (don't tell the server anything)
    private void purgeFileFromLog(String filename) {
        // java, I hate you
        for(Iterator<PendingUpdate> it = transactionLog.iterator(); it.hasNext(); )
            if(it.next().filename.equals(filename))
                it.remove();
    }
    
    // we treat any commands not surrounded by txstart and txcommit as a single
    // operation transaction
    private void ensureTransactionStart() {
        if(!processingTransaction) {
            singleOperationTransaction = true;
            txStart();
        }
    }
    
    private void ensureTransactionEnd() {
        if(singleOperationTransaction) {
            txCommit().addListener(new CallResultListener<Object>("txcommit", this));
        }
    }
    
    // update our cache whenever we get a DataResponse
    private void updateCache(DataResponse response) throws IOException {
        fileVersions.put(response.getFilename(), response.getVersion());
        services.getFileSystem().writeContents(response.getFilename(), response.getContents());
    }
    
    private Handle<Object> create(final String filename) {
        final Handle<Object> resultHandle = new Handle<Object>();
        
        ensureTransactionStart();
        
        // even though a create() is executed in a single event, we need to linearize
        // operations within a single transaction. For example, the user may execute
        //     get A
        //     create B
        // we cannot create B until A has completed, hence the lock
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                if(updateAllowed(filename, Update.Type.Create, resultHandle)) {
                    PendingUpdate update = 
                        new PendingUpdate(resultHandle, filename, 0, Update.Type.Create);
                    transactionLog.add(update);
                    fileVersions.put(filename, 0);
                    try {
                        services.getFileSystem().createFile(filename);
                    } catch (IOException e) {
                        handleUnrecoverableError(resultHandle, ErrorCode.ERR_IO_EXCEPTION);
                    }
                    ensureTransactionEnd();
                    lock.release();
                }
            }
        });
        
        return resultHandle;
    }
    
    private Handle<String> get(final String filename) {
        final Handle<String> resultHandle = new Handle<String>();
        
        ensureTransactionStart();
        
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                if(updateAllowed(filename, Update.Type.Read, resultHandle)) {
                    if(fileVersions.containsKey(filename)) {
                        localDiscOperation(DiscOperation.GET, filename, null, resultHandle);
                    } else {
                        // not in our cache
                        Handle<DataResponse> result = sendDataRequest(leaderHint, filename);
                        result.addListener(recursiveDataQueryListener(DiscOperation.GET, filename, null, resultHandle));
                    }
                }
            }
        });
        
        return resultHandle;
    }
    
    private Handle<Object> put(final String filename, final String contents) {
        final Handle<Object> resultHandle = new Handle<Object>();
        
        ensureTransactionStart();
        
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                if(updateAllowed(filename, Update.Type.Write, resultHandle)) {
                    if(fileVersions.containsKey(filename)) {
                        localDiscOperation(DiscOperation.PUT, filename, contents, resultHandle);
                    } else {
                        // not in our cache
                        Handle<DataResponse> result = sendDataRequest(leaderHint, filename);
                        result.addListener(recursiveDataQueryListener(DiscOperation.PUT, filename, contents, resultHandle));
                    }
                }
            }
        });
        
        return resultHandle;
    }
    
    private Handle<Object> append(final String filename, final String contents) {
        final Handle<Object> resultHandle = new Handle<Object>();
        
        ensureTransactionStart();
        
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                if(updateAllowed(filename, Update.Type.Write, resultHandle)) {
                    if(fileVersions.containsKey(filename)) {
                        localDiscOperation(DiscOperation.APPEND, filename, contents, resultHandle);
                    } else {
                        // not in our cache
                        Handle<DataResponse> result = sendDataRequest(leaderHint, filename);
                        result.addListener(recursiveDataQueryListener(DiscOperation.APPEND, filename, contents, resultHandle));
                    }
                }
            }
        });
        
        return resultHandle;
    }
    
    @SuppressWarnings("unchecked")
    private Handle.Listener<DataResponse> recursiveDataQueryListener(final DiscOperation operation, 
            final String filename, final String userProvidedData, final Handle resultHandle) {
        // we construct the Listener as an anonymous class rather than a normal class
        // so that we can take advantage of closures
        return new Handle.Listener<DataResponse>() {
            @Override
            public void onSuccess(DataResponse response) {
                if(response.hasForwardTo()) {
                    onDataForward(response.getForwardTo(), operation, filename, userProvidedData, resultHandle);
                } else {
                    onDataQueryResponse(operation, response, userProvidedData, resultHandle);
                }
            }
            
            @Override
            public void onError(int err) {
                if(err == ErrorCode.ERR_SERVICE_FAILURE) {
                    // resend RPCs to another server
                    onDataForward(updateLeaderHint(), operation, filename, userProvidedData, resultHandle);
                } else {
                    handleUnrecoverableError(resultHandle, err);
                }
            }
        };
    }
    
    private void onDataForward(int forwardTo, DiscOperation operation, String filename, String userProvidedData, Handle resultHandle) {
        Handle<DataResponse> result = sendDataRequest(forwardTo, filename);
        result.addListener(recursiveDataQueryListener(operation, filename, userProvidedData, resultHandle));
    }
    
    private int updateLeaderHint() {
        SortedSet<Integer> remainingPaxosNodes = services.getPaxosAddrs().tailSet(leaderHint + 1);
        if(remainingPaxosNodes.isEmpty()) {
            // we loop back around to the first node
            leaderHint = services.getPaxosAddrs().first();
        } else {
            leaderHint = remainingPaxosNodes.first(); 
        }
            
        return leaderHint;
    }
    
    // check whether the update is permitted. If it is, modify update our modification level.
    // else, return abort the transaction and return false
    private boolean updateAllowed(String filename, Update.Type update, Handle resultHandle) {
        int err;
        if((err = updateModificationLevel(filename, update)) != 0) {
            handleUnrecoverableError(resultHandle, err);
            return false;
        }
        return true;
    }
    
    private void handleUnrecoverableError(Handle resultHandle, int err) {
        resultHandle.completedError(err);
        abort();
        lock.release();
    }
    
    // APPEND is not a part of Update.Type.... so we need a local enum
    private enum DiscOperation {
        GET,
        PUT,
        APPEND;
        
        public Update.Type toUpdateType() {
            switch(this) {
            case APPEND:
            case PUT: 
                return Update.Type.Write;
            case GET: 
                return Update.Type.Read;
            }
            return null;
        }
    }
    
    private void onDataQueryResponse(DiscOperation operation, DataResponse data, String userProvidedData, Handle resultHandle) {
        try {
            updateCache(data);
        } catch (IOException e1) {
            handleUnrecoverableError(resultHandle, ErrorCode.ERR_IO_EXCEPTION);
        }
        
        localDiscOperation(operation, data.getFilename(), userProvidedData, resultHandle);
    }
    
    private void localDiscOperation(DiscOperation operation, String filename, String userProvidedData, Handle resultHandle) {
        int version = fileVersions.get(filename);
        String updatedContents = null;
        try {
            switch(operation) {
            case PUT:           
                services.getFileSystem().writeContents(filename, userProvidedData);
                updatedContents = userProvidedData;
                break;
            case GET:          
                updatedContents = services.getFileSystem().getContents(filename);
                break;
            case APPEND:        
                services.getFileSystem().appendContents(filename, userProvidedData);
                updatedContents = services.getFileSystem().getContents(filename); // hmmm, too many disc writes...
                break;
            }
        } catch(IOException e) {
            handleUnrecoverableError(resultHandle, ErrorCode.ERR_IO_EXCEPTION);
        }
        
        PendingUpdate update = 
            new PendingUpdate(resultHandle, filename, version, operation.toUpdateType(), updatedContents);
        transactionLog.add(update);
        ensureTransactionEnd();
        lock.release();
    }
    
    private Handle<Object> delete(final String filename) {
        final Handle<Object> resultHandle = new Handle<Object>();
        
        ensureTransactionStart();
        
        // even though a delete() is executed in a single event, we need to linearize
        // operations within a single transaction. For example, the user may execute
        //     get A
        //     create B
        // we cannot create B until A has completed, hence the lock
        lock.acquire(new Lock.Callback() {
            @Override
            public void run(Lock l) {
                int err;
                if((err = updateModificationLevel(filename, Update.Type.Delete)) > 0) {
                    handleUnrecoverableError(resultHandle, err);
                    return;
                }
                
                // if this is a create followed by a delete, we do /not/ want to append the update.
                // XXX this is totally bizzarre
                if(err != -1) {
                    PendingUpdate update = 
                        new PendingUpdate(resultHandle, filename, 0, Update.Type.Delete);
                    transactionLog.add(update);
                }
                fileVersions.remove(filename);
                // XXX just let the file sit on our disc...?
                ensureTransactionEnd();
                l.release();
            }
        });
        
        return resultHandle;
    }
    
    public void handleInvalidateRequest(int from, List<String> files) {
        log("received InvalidateRequest from " + from);
        
        // If we are in the middle of a transaction that uses an out-of-date
        // file, we know right away that our transaction will be aborted at the server,
        boolean foundConflict = false;
        for(String file : files) {
            fileVersions.remove(file);
            if(fileModifications.containsKey(file)) {
                foundConflict = true;
            }
            
            // XXX remove from FS?
        }
        
        if(foundConflict) {
            abort();
        }
    }

    @Override
    public void dumpInfo() {
        log("info:");
        log("     processingTransaction " + processingTransaction);
        log("     singleOperationTransaction " + singleOperationTransaction);
        log("     fileModifications: " + fileModifications);
        log("     transactionLog: " + transactionLog);
        log("     fileVersions: " + fileVersions);
        log("     lock:" + lock);
    }
}
