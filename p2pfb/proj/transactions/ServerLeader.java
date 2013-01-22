package transactions;

import primitives.Handle;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import transactions.Server.QueuedCommitAttempt;
import transactions.Server.QueuedDataRequest;

/**
 *  When the Server has synced everything
 *  we can begin acting as the Leader and responding to request normally
 */
public class ServerLeader extends ServerState {
    ServerLeader(Server server) {
        super(server);
    }

    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        Handle<DataResponse> resultHandle = new Handle<DataResponse>();
        QueuedDataRequest request = new QueuedDataRequest(from, filename, resultHandle);
        return server.sendDataResponse(resultHandle, request);
    }
    
    /**
     * If the tx hasn't already been committed, propose it as the next paxos value
     */
    public Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt) {
        Handle<CommitResponse> resultHandle = new Handle<CommitResponse>();
        
            // check if the txId has already been committed
        if(server.getFinishedTransactions().contains(txAttempt.getId())) {
            // We shouldn't just blindly give them new version numbers.. This is because there may have been other
            // transactions applied since then, and their cache copies are now out of date. In this case, to be safe, 
            // we have them flush their cache (which is achieved by sending them a CommitResponse with an empty UpdateList
            // XXX(bill): why couldn't we just give them the latest version numbers of everything?
            CommitResponse response = CommitResponse.newBuilder().build();
            return resultHandle.completedSuccess(response);
        }
        
        proposeNextPaxosInstance(txAttempt, resultHandle);
        
        return resultHandle;
    }
    
    void proposeNextPaxosInstance(CommitAttempt txAttempt, Handle<CommitResponse> resultHandle) {
        paxos.AggregateProposer aggregateProposer = server.getServices().getRole(paxos.AggregateProposer.class);
        if(aggregateProposer == null) {
            // (should never happen)
            throw new IllegalStateException("Server didn't have a corresponding proposer role!");
        }
        
        int nextPaxosId = server.getNextPaxosId();
        paxos.Proposer proposer = aggregateProposer.getIndividual(nextPaxosId);
        proposer.propose(txAttempt);
        
        server.px_queuedAttempts.put(nextPaxosId, new QueuedCommitAttempt(txAttempt, resultHandle));
    }

    @Override
    public void dumpInfo() {
        server.getServices().logPaxos(server, "currentState:   ServerLeader");
    }
}
