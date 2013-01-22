package transactions;

import primitives.Handle;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;

/**
 * Encapsulates one of the states of the Server
 * 
 * 
 * Crappy state machine:
 * 
 * 
 *      ServerSyncing  -- Syncs received or timed out -->    ServerLeader
 *          ^                                                     |
 *           |   message seen from lower paxos node              |
 * monitor      |                                                |
 *  tells us     |                                             |
 *  to be leader  -----------------> ServerReplicant  <--------
 *
 */
public abstract class ServerState {
    Server server;
    
    ServerState(Server server) {
        this.server = server;
    }
    
    public abstract Handle<DataResponse> handleDataRequest(int from, String filename);
    public abstract Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt);
    
    // precondition: !(previousState instanceof this.getClass())
    public void onEnter(ServerState previousState) { }
    // precondition: !(nextState instanceof this.getClass())
    public void onLeave(ServerState nextState) { }
    
    // Hmmmmmm, seems a little out of place....  the superclass shouldn't know anything about its subclasses...
    public boolean actingAsLeader() {
        return this instanceof ServerLeader;
    }
    
    public boolean actingAsReplicant() {
        return this instanceof ServerReplicant;
    }
    
    public boolean actingAsSyncing() {
        return this instanceof ServerSyncing;
    }
    
    public abstract void dumpInfo();
}