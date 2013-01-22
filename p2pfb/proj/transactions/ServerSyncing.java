package transactions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import primitives.Handle;
import proto.RPCProtos.ChosenTransaction;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import transactions.Server.QueuedDataRequest;
import transactions.Server.QueuedCommitAttempt;

/**
 * If the monitor has decided that we should become the leader, 
 * we need to first make sure that we're up to speed with all the transactions
 * that have been committed. So we send SyncRequests to all nodes, and apply anything
 * they've learned about, or timeout on the request. In either case, we'll move to the leader
 * state fairly quickly.
 * 
 * This isn't strictly necessary -- safety is assured in any case. But it could be
 * realllllly inefficient if the client had to time out "gap" times before it was able 
 * to commit a transaction
 *
 */
public class ServerSyncing extends ServerState {
    private Set<Integer> pendingSyncs = new HashSet<Integer>(); 
    // data requests that have are pending until we get back in sync
    private List<QueuedDataRequest> queuedDataRequests = new ArrayList<QueuedDataRequest>();
    private List<QueuedCommitAttempt> queuedCommitAttempts = new ArrayList<QueuedCommitAttempt>();
    
    ServerSyncing(Server server) {
        super(server);
    }
    
    @Override
    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        Handle<DataResponse> resultHandle = new Handle<DataResponse>();
        queuedDataRequests.add(new QueuedDataRequest(from, filename, resultHandle));
        return resultHandle;
    }
    
    @Override
    public Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt) {
        Handle<CommitResponse> resultHandle = new Handle<CommitResponse>();
        queuedCommitAttempts.add(new QueuedCommitAttempt(txAttempt, resultHandle));
        return resultHandle;
    }
    
    @Override // should this just be done in the constructor?
    public void onEnter(ServerState previousState) {
        for(Integer node : server.getServices().getPaxosAddrs()) {
            if(node != server.rpcNode.addr)
                pendingSyncs.add(node);
        }

        for(final Integer node : pendingSyncs) {
           // send out sync messages to all of the other managers
           Handle<List<ChosenTransaction>> requestHandle = server.rpcNode.sendSyncRequest(node);
           requestHandle.addListener(new Handle.Listener<List<ChosenTransaction>>() {
               // aggregate the results; when we receive a majority, try to commit those
               // transactions and change state -> LEADER
               @Override
               public void onSuccess(List<ChosenTransaction> appliedTransactions) {
                   for(ChosenTransaction applied : appliedTransactions) {
                       server.onValueChosen(applied.getPxid(), applied.getTx());
                   }
                   
                   removeNodeFromPending();
               }
               
               @Override
               public void onError(int errorCode) {
                   removeNodeFromPending();
               }
               
               private void removeNodeFromPending() {
                   pendingSyncs.remove(node);
                   if(pendingSyncs.isEmpty()) {
                       // we've gotten responses or timed out on all servers, so 
                       // change state to leader. XXX stop at a majority?
                       server.changeState(new ServerLeader(server));
                   }
               }
           });
        }
    }
    
    @Override
    public void onLeave(ServerState nextState) {
        if(nextState.actingAsLeader()) {
            // SYNCING -> LEADER
            ServerLeader newLeader = (ServerLeader) nextState;
            // respond to all of the queued data requests
            for(QueuedDataRequest request : queuedDataRequests) {
                server.sendDataResponse(request.resultHandle, request);
            }
            // find paxos ids for all of the queuedCommitRequests
            for(QueuedCommitAttempt attempt : queuedCommitAttempts) {
                newLeader.proposeNextPaxosInstance(attempt.attempt, attempt.resultHandle);
            }
        } else if(nextState.actingAsReplicant()) {
            // SYNCING -> REPLICANT
            // we are no longer about to become the leader, so forward the requests to whoever we now
            // think the leader is
            for(QueuedDataRequest request : queuedDataRequests) {
                server.forwardDataRequest(request.resultHandle);
            }
            for(QueuedCommitAttempt attempt : queuedCommitAttempts) {
                server.forwardCommitAttempt(attempt.resultHandle);
            }
        }
    }
    
    @Override
    public void dumpInfo() {
        server.getServices().logPaxos(server, "currentState:   ServerSyncing");
        server.getServices().logPaxos(server, "       pendingSyncs:         " + pendingSyncs);
        server.getServices().logPaxos(server, "       queuedCommitAttempts: " + queuedCommitAttempts);
        server.getServices().logPaxos(server, "       queuedDataRequests:   " + queuedDataRequests);
    }
}
