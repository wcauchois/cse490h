package transactions;

import java.util.List;

import main.RPCNode;

import paxos.Role;
import paxos.Services;
import primitives.Handle;
import primitives.Logger;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.Update;
import proto.RPCProtos.VersionUpdate;

/**
 * Abstarct superclass for Server and Client
 *
 */
public abstract class TransactionBase extends Role implements Logger {
    protected RPCNode rpcNode;
    public TransactionBase(RPCNode node, Services services) {
        super(node, services);
        this.rpcNode = node;
    }
    
    public abstract void onCommand(String cmd);
    
    public Handle<DataResponse> sendDataRequest(int dest, String filename) {
        return rpcNode.sendDataRequest(dest, filename);
    }
    public Handle<CommitResponse> sendCommitAttempt(int dest, int txId, List<Update> updates) {
        return rpcNode.sendCommitAttempt(dest, txId, updates);
    }
    
    public void sendInvalidateRequest(int dest, List<String> files) {
        rpcNode.sendInvalidateRequest(dest, files);
    }
    
    public void log(String msg) {
        rpcNode.log(msg);
    }
    
    public void logError(String msg) {
        rpcNode.logError(msg);
    }
    
    public void logSuccess(String msg) {
        rpcNode.logSuccess(msg);
    }
}
