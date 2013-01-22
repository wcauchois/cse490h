package transactions;

import java.io.IOException;
import java.util.*;

import com.google.protobuf.TextFormat;
import edu.washington.cs.cse490h.lib.Utility;
import main.ErrorCode;
import main.RPCNode;

import paxos.Monitor;
import paxos.Services;
import paxos.Util;
import primitives.Handle;
import proto.RPCProtos.ChosenTransaction;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.Update;
import proto.RPCProtos.VersionUpdate;

/**
 * Replicated Paxos Server. Applies paxos learned values and handles RPCs from clients. 
 *  
 * When we reboot, apply our undo log and recover our old state consistently
 */
public class Server extends TransactionBase {
    private UndoRedoLog log;
    private Map<String, FileInfo> fileTable = new HashMap<String, FileInfo>();
    private CacheUndoRedoer undoRedoer = new CacheUndoRedoer(); // only need one instance...
    public static final String LOG_FILENAME = "server.log";
    public static final String FILE_TABLE_FILENAME = "server.tbl";
    
    Services getServices() {
        return services;
    }
    
    // serves a dual purpose: 
    //   i.) when we're the leader and we learn that this tx was chosen by paxos, we need
    //      to grab the resultHandle to notify the client that we've executed succesfully
    //   ii.) if a NOP was chosen for the px instantiation we intended for this transaction,
    //        we can grab the original CommitAttempt and re-propose it for another paxos instantiation
    static class QueuedCommitAttempt {
        public CommitAttempt attempt;
        public Handle<CommitResponse> resultHandle;
        
        public QueuedCommitAttempt(CommitAttempt attempt, Handle<CommitResponse> resultHandle) {
            this.attempt = attempt;
            this.resultHandle = resultHandle;
        }
    }
    
    static class QueuedDataRequest {
        public int from;
        public String filename;
        public Handle<DataResponse> resultHandle;
        
        public QueuedDataRequest(int from, String filename, Handle<DataResponse> resultHandle) {
            this.from = from;
            this.filename = filename;
            this.resultHandle = resultHandle;
        }
    }
    
    
    // transactions that have been applied on this server (where the position
    // in the list denotes the paxos instance ID). 
    List<CommitAttempt> px_appliedTransactions = new ArrayList<CommitAttempt>();
    
    // return the set of transaction ids that have been applied on this server
    Set<Integer> getFinishedTransactions() {
        Set<Integer> result = new TreeSet<Integer>();
        for(CommitAttempt c : px_appliedTransactions)
            result.add(c.getId());
        return result;
    }
        
    // transactions that have been chosen by paxos, but not yet applied on this
    // server (i.e. there is a gap in paxos instance IDs).
    private TreeMap<Integer, CommitAttempt> px_pendingTransactions 
        = new TreeMap<Integer, CommitAttempt>(); // pxid -> chosen transaction
    
    /**
     * @return  the set of paxos instantiations which have not yet decided on a value
     */
    // TODO: should this state be managed by paxos.Learner, since he is the one timeing out
    // on paxos?
    public Map<Integer, CommitAttempt> getUnchosenPaxosIds() {
        Map<Integer, CommitAttempt> unchosenPaxosValues = new HashMap<Integer, CommitAttempt>();
        for(Map.Entry<Integer, QueuedCommitAttempt> entry : px_queuedAttempts.entrySet()) {
            unchosenPaxosValues.put(entry.getKey(), entry.getValue().attempt);
        }
        
        // if we recently crashed, there might also be empty gaps that we don't
        // have stored in px_queuedAttempts. we still need to be sure to 
        // fill these gaps with NOPs...
        // (We want to return the "gaps" between applied transactions and pending transactions
        // in Ruby..., this would be a one liner:)
        //    return (appliedTransactions.size...nextPaxosId) - pendingTransactions.keys
        for(int pxId = px_appliedTransactions.size(); pxId < getNextPaxosId(); pxId++) {
            if(!unchosenPaxosValues.containsKey(pxId))
                unchosenPaxosValues.put(pxId, paxos.Util.NOP);
        }
        
        return unchosenPaxosValues;
    }
    
    // commit attempts that have yet to be chosen by paxos, but which have been
    // submitted to this server. We map from pxid -> unchosen transaction so that
    // we can recover and re-propose any transaction for which paxos chose NOP
    // invariant: queuedAttempts.size() == 0 if state == REPLICANT
    SortedMap<Integer, QueuedCommitAttempt> px_queuedAttempts = 
        new TreeMap<Integer, QueuedCommitAttempt>(); // pxid -> unchosen transaction
    
    private ServerState currentState = new ServerReplicant(this);
    
    private <T extends ServerState> T getState(Class<T> klass) {
       if(currentState.getClass().equals(klass))
          return (T) currentState;
       else 
          throw new IllegalStateException("Not in the correct state!");
    }
    
    void changeState(ServerState state) {
        ServerState oldState = currentState;
        oldState.onLeave(state);
        currentState = state;
        currentState.onEnter(oldState);
    }
    
    private static class FileInfo {
        private String filename;
        private int version;
        public List<Integer> copyset;
        
        public int getVersion() {
            return version;
        }
        public int incrementVersion() {
            return ++version;
        }
        public void revert(int oldVersion) {
            version = oldVersion;
        }
        public String getFilename() {
            return filename;
        }
        
        public FileInfo(String filename) {
            this(filename, 0);
        }
        public FileInfo(String filename, int version) {
            this.filename = filename;
            this.version = version;
            this.copyset = new ArrayList<Integer>();
        }
        
        public Messages.FileInfo toProto() {
            return Messages.FileInfo.newBuilder()
                .setFilename(filename)
                .setVersion(version)
                .addAllCopyset(copyset)
                .build();
        }
        
        public static FileInfo fromProto(Messages.FileInfo proto) {
            FileInfo info = new FileInfo(proto.getFilename(), proto.getVersion());
            info.copyset.addAll(proto.getCopysetList());
            return info;
        }
    }
    
    public Server(RPCNode node, Services services) { // on boot (gets called in Node.start)
        super(node, services);
        
        log = new UndoRedoLog(n, LOG_FILENAME);
        // we gotta restore the file table before we recover
        if(Utility.fileExists(n, FILE_TABLE_FILENAME))
            loadFileTable();
        log.recover(undoRedoer);
    }
    
    // return the id of the next instance of paxos to run
    int getNextPaxosId() {
        if(!px_queuedAttempts.isEmpty()) {
            // we are currently running paxos -- so choose the next pxid
            return px_queuedAttempts.lastKey() + 1;
        } else if(px_pendingTransactions.isEmpty()) {
            // there are no transactions that have been chosen, but not applied; the
            // next paxos id is just the size of the appliedTransactions list
            return px_appliedTransactions.size();
        } else {
            // else, return the greatest instance id in the queue -- plus 1
            return px_pendingTransactions.descendingKeySet().first() + 1;
        }
    }
    
    private boolean saveFileTable() {
        Messages.FileTable.Builder builder = Messages.FileTable.newBuilder();
        for(FileInfo info : fileTable.values())
            builder.addFiles(info.toProto());
        try {
            String encodedFileTable = builder.build().toString();
            services.getFileSystem().writeContents(FILE_TABLE_FILENAME, encodedFileTable);
        } catch(IOException e) {
            return false;
        }
        return true;
    }
    
    private void loadFileTable() {
        fileTable.clear();
        try {
            Messages.FileTable.Builder builder = Messages.FileTable.newBuilder();
            String encodedFileTable = services.getFileSystem().getContents(FILE_TABLE_FILENAME);
            TextFormat.merge(encodedFileTable, builder);
            Messages.FileTable data = builder.build();
            for(int i = 0; i < data.getFilesCount(); i++) {
                FileInfo info = FileInfo.fromProto(data.getFiles(i));
                fileTable.put(info.getFilename(), info);
            }
        } catch(IOException e) {
            System.err.println("failed to load the file table from disk!");
            n.fail();
        }
    }

    @Override
    public void onCommand(String cmd) {
        if(cmd.startsWith("info")) {
            dumpInfo();
        } else
            logError("unrecognized command");
    }
    
    private class CacheUndoRedoer implements UndoRedoLog.UndoRedoer {
        private Map<Integer, CommitAttempt.Builder> commits =
            new TreeMap<Integer, CommitAttempt.Builder>();
        private CommitAttempt.Builder currentCommit;
        
        public void undoWrite(String filename, int oldVersion, String oldContents) {
            log("undoing write of " + filename + " (revert to v" + oldVersion + ")");
            fileTable.get(filename).revert(oldVersion);
            try {
                services.getFileSystem().writeContents(filename, oldContents);
            } catch(IOException e) { /* balls */ }
        }
        public void undoDelete(String filename, int oldVersion, String oldContents) {
            log("undoing delete of " + filename + " (revert to v" + oldVersion + ")");
            // the opposite of a delete is a create (profound, i know)
            fileTable.put(filename, new FileInfo(filename, oldVersion));
            try {
                services.getFileSystem().createFile(filename);
                services.getFileSystem().writeContents(filename, oldContents);
            } catch(IOException e) { /* balls */ }
        }
        public void undoCreate(String filename) {
            log("undoing create of " + filename);
            // the opposite of a create is a delete
            fileTable.remove(filename);
            try {
                services.getFileSystem().deleteFile(filename);
            } catch(IOException e) { /* balls */ }
        }
        
        @Override
        public void beginRedo(int pxId) {
            currentCommit = CommitAttempt.newBuilder();
            commits.put(pxId, currentCommit);
        }
        @Override
        public void endRedo(int pxId) {
            currentCommit = null;
        }
        
        @Override
        public void redoCreate(String filename) {
            currentCommit.addUpdates(Update.newBuilder()
                    .setType(Update.Type.Create)
                    .setFilename(filename)
                    // XXX what about the version??
                    );
        }
        @Override
        public void redoDelete(String filename, int oldVersion,
                String oldContents) {
            currentCommit.addUpdates(Update.newBuilder()
                    .setType(Update.Type.Delete)
                    .setFilename(filename)
                    .setVersion(oldVersion)
                    .setContents(oldContents)
                    );
        }
        @Override
        public void redoWrite(String filename, int oldVersion,
                String oldContents) {
            currentCommit.addUpdates(Update.newBuilder()
                    .setType(Update.Type.Write)
                    .setFilename(filename)
                    .setVersion(oldVersion)
                    .setContents(oldContents)
                    );
        }
        
        @Override
        public void onRecoveryComplete() {
            updateChosenTransactions();
        }
        
        // add all the redo'd commits to the appliedTransactions list
        private void updateChosenTransactions() {
            Set<Integer> expectedRange = new TreeSet<Integer>();
            for(int i = 0; i < commits.size(); i++)
                expectedRange.add(i);

            if(!commits.keySet().equals(expectedRange)) {
                throw new RuntimeException("what does this mean??"); // XXX
            }
            
            px_appliedTransactions = new ArrayList<CommitAttempt>(commits.size());
            for(int i : commits.keySet())
                px_appliedTransactions.add(commits.get(i).build());
        }
    }
    
    // invoked by the monitor when this server becomes a leader
    public void ensureLeader() {
        if(currentState.actingAsReplicant())
            changeState(new ServerSyncing(this));
    }
    
    // invoked by the monitor (or Role) when this server stops being a leader -- i.e., when
    // we receive a message from a paxos node with an addr < than ours.
    public void relinquishLeadership() {
        if(currentState.actingAsLeader() || currentState.actingAsSyncing())
            changeState(new ServerReplicant(this));
    }
    
    public Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt) {
        return currentState.handleCommitAttempt(from, txAttempt);
    }
    
    private boolean areVersionsCorrect(List<Update> updateList, int pxId /* for the error message */) {
        for(Update update : updateList) {
            switch(update.getType()) {
            case Read:
            case Write:
            case Delete:
                FileInfo info = fileTable.get(update.getFilename());
                if(info != null && update.getVersion() != info.getVersion()) {
                    logError("transaction ##" + pxId + " failed: version mismatch on " + update.getFilename());
                    logError("\tgot " + update.getVersion() + ", expected " + info.getVersion());
                    return false;
                } // else, if info == null, then it has to be created in this tx
                break;
            case Create:
                break;
            }
        }
        return true;
    }
    
    Handle<CommitResponse> executeTransaction(int from, int pxId, CommitAttempt txAttempt, Handle<CommitResponse> resultHandle) {
        List<Update> updates = txAttempt.getUpdatesList();
        
        boolean alreadyApplied = getFinishedTransactions().contains(txAttempt.getId());
        px_appliedTransactions.add(txAttempt); // we add no matter what to make sure there are not gaps
        
        if(alreadyApplied) {
            // we already applied this transaction!!!
            // what the hell should I do here...
            // we add it to px_appliedTransactions to make sure that
            // we can proceed with the subsequent transactions.
            // but we can't apply it, since we've already applied it.
            // do we tell the user that there was an error? there wasn't an error
            // /in their transaction/, there was just an error in that we accidentally
            // chose the trasnaction as a paxos value twice due to server failures.
            logError("Already applied the transaction " + txAttempt.getId() + " ... refusing to apply again");
            return resultHandle; // we aren't upholding our promise here!!!
        }  // it's unfortunate that getFinishedTransactions depends on px_appliedTransactions.... can't factor out
        
        // first, make sure every update is based off the right version
        if(!areVersionsCorrect(txAttempt.getUpdatesList(), pxId))
            return resultHandle.completedError(ErrorCode.ERR_TRANSACTION_FAILED);
        
        
        log("executing transaction ##" + pxId + " (# of updates is " + updates.size() + ")");
        // next, iterate through the updates again and try to perform the actions
        log.logTxStart(pxId);
        
        // mapping from file names to their copy sets. we need the copy set in this datastructure since
        // for a delete operation, we delete the FileInfo object from the fileTable.
        // XXX: is there a better way than "inverting" this map?
        Map<String, List<Integer>> filesToInvalidate = new TreeMap<String, List<Integer>>();
        List<VersionUpdate> versionUpdates = new ArrayList<VersionUpdate>();
        try {
            for(Update update : updates) {
                String filename = update.getFilename(), oldContents;
                FileInfo info;
                switch(update.getType()) {
                case Read:
                    // do nothing; the read was already performed on the client
                    break;
                // for write, create, and delete: update the server's copy to reflect the modifications
                case Write:
                    if(!fileTable.containsKey(filename)) {
                        logError("transaction ##" + pxId + " failed: file " + filename + " not found");
                        log.txAbort(undoRedoer);
                        return resultHandle.completedError(ErrorCode.ERR_FILE_NOT_FOUND);
                    }
                    
                    log("\twrite to " + update.getFilename() + ": " + update.getContents());
                    oldContents = services.getFileSystem().getContents(filename);
                    // log the write before we update the services.getFileSystem()
                    log.logWrite(filename, update.getVersion(), oldContents);
                    info = fileTable.get(filename);
                    int newVersion = info.incrementVersion(); // update our in-memory representation!
                    versionUpdates.add(VersionUpdate.newBuilder()
                            .setFilename(filename)
                            .setVersion(newVersion)
                            .build());

                    // we're gonna wanna invalidate the copyset, and the old owner
                    filesToInvalidate.put(filename, info.copyset);
                    
                    services.getFileSystem().writeContents(filename, update.getContents());
                    break;
                case Create:
                    if(fileTable.containsKey(filename)) {
                        logError("transaction ##" + pxId + " failed: file " + filename + " exists");
                        log.txAbort(undoRedoer);
                        return resultHandle.completedError(ErrorCode.ERR_FILE_EXISTS);
                    }
                    
                    log("\tcreate of " + update.getFilename());
                    // log the create before we update the services.getFileSystem()
                    log.logCreate(filename);
                    // update our in-memory representation!
                    // create a new FileInfo object with "from" as the owner!
                    fileTable.put(filename, info = new FileInfo(filename));
                    info.copyset.add(from); // add the originator to the copyset
                    services.getFileSystem().createFile(filename);
                    
                    if(update.hasContents() && !update.getContents().isEmpty()) {
                        log("\t\twith contents: " + update.getContents());
                        // we are creating the file with some predefined contents...
                        services.getFileSystem().writeContents(filename, update.getContents());
                    }
                    // we don't need to add to filesToInvalidate since the copyset is the empty set
                    break;
                case Delete:
                    if(!fileTable.containsKey(filename)) {
                        logError("transaction ##" + pxId + " failed: file " + filename + " not found");
                        log.txAbort(undoRedoer);
                        return resultHandle.completedError(ErrorCode.ERR_FILE_NOT_FOUND);
                    }
                    
                    log("\tdelete of " + update.getFilename());
                    oldContents = services.getFileSystem().getContents(filename);
                    log.logDelete(filename, update.getVersion(), oldContents);
                    info = fileTable.remove(filename); // update our in-memory representation!
                    // we're gonna wanna invalidate everyone
                    filesToInvalidate.put(filename, info.copyset);
                    services.getFileSystem().deleteFile(filename);
                    break;
                }
            }
        } catch(IOException e) {
            logError("\tencountered IO exception... abort");
            log.txAbort(undoRedoer);
            return resultHandle.completedError(ErrorCode.ERR_IO_EXCEPTION);
        }
        
        saveFileTable(); // save the file table right before we commit
        // XXX: is this the right place for that??
        
        logSuccess("COMMIT");
        log.logTxCommit();

        // okay, now we need to invalidate all the files we modified
        log("invalidating copysets...");
        
        // convert the mapping from files to copysets to a mapping from clients to which files should be
        // invalidated on that client (corresponding to a single InvalidateRequest messsage).
        Map<Integer, List<String>> invalidateRequests = new TreeMap<Integer, List<String>>();
        for(String filename : filesToInvalidate.keySet()) {
            List<Integer> copyset = filesToInvalidate.get(filename);
            for(Integer client : copyset) {
                List<String> files;
                if(!invalidateRequests.containsKey(client)) {
                    invalidateRequests.put(client, files = new ArrayList<String>());
                } else {
                    files = invalidateRequests.get(client);
                }
                files.add(filename);
            }
            if(fileTable.containsKey(filename)) {
                // the file may not be in the fileTable, if it was deleted...
                
                // IMPORTANT: do not remove the originator (from) from the copyset!!
                List<Integer> copysetPrime = fileTable.get(filename).copyset;
                boolean containsFrom = copysetPrime.contains(from);
                copysetPrime.clear();
                if(containsFrom) copysetPrime.add(from);
            }
        }
        
        if(invalidateRequests.isEmpty())
            logSuccess("nothing to invalidate");
        // now send out the invalidate requests
        for(Integer client : invalidateRequests.keySet()) {
            if(client.intValue() == from)
                continue;
            // don't need to wait for the invalidate request to complete... the worst that could happen
            // is that somebody would have to roll back
            sendInvalidateRequest(client, invalidateRequests.get(client));
            log("\tsent invalidate to " + client + " of " +
                    Arrays.toString(invalidateRequests.get(client).toArray()));
        }
        
        logSuccess("DONE");
        
        CommitResponse response = CommitResponse.newBuilder()
            .addAllUpdatedVersions(versionUpdates)
            .build();
        
        return resultHandle.completedSuccess(response);
    }

    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        return currentState.handleDataRequest(from, filename);
    }
    
    Handle<DataResponse> sendDataResponse(Handle<DataResponse> resultHandle, QueuedDataRequest request) {
        DataResponse.Builder responseBuilder = DataResponse.newBuilder();

        if(!fileTable.containsKey(request.filename)) {
            return resultHandle.completedError(ErrorCode.ERR_FILE_NOT_FOUND);
        }
        FileInfo info = fileTable.get(request.filename);
        responseBuilder.setVersion(info.getVersion());
        responseBuilder.setFilename(request.filename);
        try {
            responseBuilder.setContents(services.getFileSystem().getContents(request.filename));
        } catch(IOException e) {
            return resultHandle.completedError(ErrorCode.ERR_IO_EXCEPTION);
        }
        info.copyset.add(request.from); // add the requester to this file's copyset
        log("handled data request of " + request.filename + "(" + info.getVersion() + ") from " + request.from);
        return resultHandle.completedSuccess(responseBuilder.build());
    }
    
    Handle<DataResponse> forwardDataRequest(Handle<DataResponse> resultHandle) {
        Monitor monitor = services.getRole(Monitor.class);

        DataResponse forwardResponse = DataResponse.newBuilder()
            .setForwardTo(monitor.getLeader())
            .build();
        return resultHandle.completedSuccess(forwardResponse);
    }
    
    Handle<CommitResponse> forwardCommitAttempt(Handle<CommitResponse> resultHandle) {
        Monitor monitor = services.getRole(Monitor.class);

        CommitResponse forwardResponse = CommitResponse.newBuilder()
             .setForwardTo(monitor.getLeader())
             .build();
            
        return resultHandle.completedSuccess(forwardResponse);
    }

    // TODO: what if V is a NOP? If V is a NOP, the txId that the user originally specified for this intance
    //       of PAXOS will /not/ appear in getFinishedTransactions().
    public void onValueChosen(int pxId, CommitAttempt v) {
        // Will this ever be called twice for the same pxId?
        // make sure we don't apply a txid more than once..
        if(pxId >= px_appliedTransactions.size()) {
            // invariant: px_pendingTransactions.firstKey() > px_appliedTransactions.size()
            px_pendingTransactions.put(pxId, v);
            flushPendingTransactions();
        }
    }

    // Apply the transactions which we are permitted to apply (avoid gaps)
    private void flushPendingTransactions() {
        int nextUnappliedPxId = px_appliedTransactions.size();
        
        while(!px_pendingTransactions.isEmpty() && nextUnappliedPxId == px_pendingTransactions.firstKey()) {
            // this is the next transaction to execute!
            CommitAttempt v = px_pendingTransactions.remove(nextUnappliedPxId);
            
            Handle<CommitResponse> resultHandle;
            
            QueuedCommitAttempt queuedAttempt = px_queuedAttempts.remove(nextUnappliedPxId);
                        
            if(queuedAttempt != null && queuedAttempt.attempt.equals(v)) {
                // this was the right CommitAttempt, so notify the client when we're done applying it
                resultHandle = queuedAttempt.resultHandle;
            } else {
                // either the value was a NOP, some other transaction ended up being chosen, 
                //    or we aren't the leader (we're just learning and applying transactions)
                // in any of these cases, we just create an empty result handle
                resultHandle = new Handle<CommitResponse>();
            }
            
            executeTransaction(v.getFrom(), nextUnappliedPxId, v, resultHandle);
            
            // (we need to run this after executeTransaction so that the correct paxosId is used)
            if(currentState.actingAsLeader() && queuedAttempt != null && !queuedAttempt.attempt.equals(v)) {
                // We had a queued transaction for this paxos instantiation, but it didn't get chosen...
                // so we need to reassign it to another paxos instantiation
                getState(ServerLeader.class)
                    .proposeNextPaxosInstance(queuedAttempt.attempt, queuedAttempt.resultHandle);
            }
            
            nextUnappliedPxId++;
        }
    }

    public List<ChosenTransaction> handleSyncRequest() {
        List<ChosenTransaction> applied = new ArrayList<ChosenTransaction>();
        for(int i = 0; i < px_appliedTransactions.size(); i++) {
            applied.add(chosenTransaction(i, px_appliedTransactions.get(i)));
        }
        for(Map.Entry<Integer, CommitAttempt> entry : px_pendingTransactions.entrySet()) {
            applied.add(chosenTransaction(entry.getKey(), entry.getValue()));
        }
        return applied;
    }
    
    // protobuf doesn't support hashmaps... so we need to flatten the hashmap into a list of ChosenTransactions
    // it would be sorta cool if protobuf supported normal contstructors too... I guess they don't know the invariants beforehand though,
    // which is the whole point of the builder
    private ChosenTransaction chosenTransaction(int pxId, CommitAttempt tx) {
        return ChosenTransaction.newBuilder()
            .setPxid(pxId)
            .setTx(tx)
            .build();
    }
    
    @Override
    public void dumpInfo() {
        services.logPaxos(this, "contents of the file table:");
        for(String filename : fileTable.keySet()) {
            FileInfo info = fileTable.get(filename);
            log("\t" + filename + "(" + info.getVersion() + ") has copyset "
                    + Arrays.toString(info.copyset.toArray()));
        }
        
        services.logPaxos(this, "appliedTransactions: " + px_appliedTransactions);
        services.logPaxos(this, "pendingTransactions: " + px_pendingTransactions);
        services.logPaxos(this, "queuedAttempts: " + px_queuedAttempts);
        currentState.dumpInfo();
    }
}
