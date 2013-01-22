package transactions;

import paxos.Monitor;
import primitives.Handle;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;

/**
 * Just a hum-ho server replicant. Apply learned paxos transactions, but forward all requests
 *
 */
class ServerReplicant extends ServerState {
    ServerReplicant(Server server) {
        super(server);
    }

    // we need to send a forwardTo reply 
    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        Handle<DataResponse> resultHandle = new Handle<DataResponse>();
        return server.forwardDataRequest(resultHandle);
    }
    
    public Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt attempt) {
        Handle<CommitResponse> resultHandle = new Handle<CommitResponse>();
        return server.forwardCommitAttempt(resultHandle);
    }
    

    @Override
    public void dumpInfo() {
        server.getServices().logPaxos(server, "currentState:   ServerReplicant");
    }
}
