package main;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

import primitives.Handle;
import primitives.Logger;
import proto.RPCProtos.ChosenTransaction;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.RPCEnvelope;
import static proto.RPCProtos.*;  
import static paxos.Messages.*;

/**
 * Builds an RPC stub layer on top of the reliable in-order message layer. Subclasses act as both
 * RPC callers and RPC callees. In order to call an RPC method, simply invoke the appropriate
 * call* method -- and then use the returned Handle to get the result. Subclasses must also
 * implement the abstract handle* methods to handle RPCs from other peers. These methods also
 * return Handles, so that long-running computations will not block the messaging layer.
 */
public abstract class RPCNode extends RIONode implements Logger {
    // ======================================================================
    // subclasses must provide implementations of these methods for the stub layer to call
    // ======================================================================
    // Server handlers:
    public abstract Handle<DataResponse> handleDataRequest(int from, String filename);
    public abstract Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt);
    public abstract List<ChosenTransaction> handleSyncRequest(); // should this return a Handle?

    // Client handlers:
    // no Invalidate confirm needed... the worst that can happen is that we'll have our transactions aborted
    public abstract void handleInvalidateRequest(int from, List<String> files);
    
    // ======================================================================
    
    // Proxies for client.send* methods
    // ======================================================================
    // Manager stubs:
    public void sendInvalidateRequest(int dest, List<String> files) {
        client.sendInvalidateRequest(dest, files);
    }
    public Handle<List<ChosenTransaction>> sendSyncRequest(int dest) {
        return client.sendSyncRequest(dest);
    }
    // Client stubs:
    public Handle<DataResponse> sendDataRequest(int dest, String filename) {
        return client.sendDataRequest(dest, filename);
    }
    
    public Handle<CommitResponse> sendCommitAttempt(int dest, int txId, List<Update> updates) {
        return client.sendCommitAttempt(dest, txId, updates);
    }
    // ======================================================================
    
    private RPCServer server;
    private RPCClient client;
    
    /**
     * ctor
     */
    public RPCNode() {
        this.server = new RPCServer(this);
        this.client = new RPCClient(this);  
    }

    @Override
    public void onRIOReceive(Integer from, int appProtocol, byte[] msg) {
        if(appProtocol != Protocol.RPC) {
            logError("received non-RPC packet; ignoring it");
            return;
        }
        RPCEnvelope envelope;
        try {
            envelope = RPCEnvelope.parseFrom(msg);
            RPCEnvelope.Type type = envelope.getType();
            switch(type) {
                // Dispatched to RPCServer
                case DataRequest:               server.receiveDataRequest(from, envelope); break;
                case CommitAttempt:             server.receiveCommitAttempt(from, envelope); break;
                case InvalidateRequest:         server.receiveInvalidateRequest(from, envelope); break;
                case SyncRequest:               server.receiveSyncRequest(from, envelope); break;
                // Dispatched to RPCClient
                case DataResponse:              client.receiveDataResponse(from, envelope) ; break;
                case CommitResponse:            client.receiveCommitResponse(from, envelope); break;
                case SyncResponse:              client.receiveSyncResponse(from, envelope); break;
                default:                        logError("received unknown RPC type: " + type);
            }
        } catch (InvalidProtocolBufferException e) {
            log("invalid RPC envelope");
        }
    }
    
    @Override
    public void onPeerFailure(int peer, Collection<RIOPacket> failedSends) {
        for(RIOPacket pkt : failedSends) {
            if(pkt.getProtocol() == Protocol.RPC) {
                RPCEnvelope envelope = null;
                try {
                    envelope = RPCEnvelope.parseFrom(pkt.getPayload());
                } catch (InvalidProtocolBufferException e) {
                    logError("Invalid RPC packet queued in onPeerFailure");
                }
                
                switch(envelope.getType()) {
                case DataRequest:
                case CommitAttempt:
                case SyncRequest:
                    // this is a call packet that didn't make it to the sever... we should
                    // notify clients of RPCClient that their call has failed
                    client.onCallFailure(envelope.getCallID());
                    break;
                }
            }
        }
    }

}
