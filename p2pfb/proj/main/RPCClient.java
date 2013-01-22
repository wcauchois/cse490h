package main;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import primitives.Handle;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.RPCEnvelope;
import proto.RPCProtos.Update;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import static proto.RPCProtos.*;

/**
 * The set of client RPC stubs
 *
 */
@SuppressWarnings("unchecked")
public class RPCClient {
    private RPCNode parent;
    private int nextCallID = 0;
    // callID()s are used to associate RPC returns with the correct handle
    private Map<Integer, Handle> ongoingCalls = new HashMap<Integer, Handle>();
    
    /**
     * Constructor
     * 
     * @param rpcNode a reference to the user of these RPCs
     */
    public RPCClient(RPCNode parent) {
        this.parent = parent;
    }
    
    public Handle<DataResponse> sendDataRequest(int dest, String filename) {
        DataRequest dataRequest = DataRequest.newBuilder()
            .setFilename(filename)
            .build();
        RPCEnvelope envelope = newEnvelope(RPCEnvelope.Type.DataRequest, dataRequest);
        sendEnvelope(dest, envelope);
    
        Handle<DataResponse> resultHandle = new Handle<DataResponse>();
        ongoingCalls.put(envelope.getCallID(), resultHandle);
        return resultHandle;
    }
    
    public Handle<CommitResponse> sendCommitAttempt(int dest, int txId, List<Update> updates) {
        CommitAttempt commitAttempt = CommitAttempt.newBuilder()
            .addAllUpdates(updates)
            .setFrom(parent.addr)
            .setId(txId)
            .build();
        RPCEnvelope envelope = newEnvelope(RPCEnvelope.Type.CommitAttempt, commitAttempt);
        sendEnvelope(dest, envelope);

        Handle<CommitResponse> resultHandle = new Handle<CommitResponse>();
        ongoingCalls.put(envelope.getCallID(), resultHandle);
        return resultHandle;
    }
    
    public Handle<List<ChosenTransaction>> sendSyncRequest(int dest) {
        SyncRequest request = SyncRequest.newBuilder().build();
        RPCEnvelope envelope = newEnvelope(RPCEnvelope.Type.SyncRequest, request);
        sendEnvelope(dest, envelope);

        Handle<List<ChosenTransaction>> resultHandle = new Handle<List<ChosenTransaction>>();
        ongoingCalls.put(envelope.getCallID(), resultHandle);
        return resultHandle;
    }

    public void sendInvalidateRequest(int dest, List<String> files) {
        InvalidateRequest invalidateRequest = InvalidateRequest.newBuilder()
            .addAllFiles(files)
            .build();
        
        RPCEnvelope envelope = newEnvelope(RPCEnvelope.Type.InvalidateRequest, invalidateRequest);
        sendEnvelope(dest, envelope);
    }
    
    public void receiveSyncResponse(Integer from, RPCEnvelope envelope) {
        try {
            Handle uncastHandle = ongoingCalls.remove(envelope.getCallID());
            if(uncastHandle == null) {
                parent.logError("RPC client got invalid callID from " + from);
                return;
            }
            Handle<List<ChosenTransaction>> handle = (Handle<List<ChosenTransaction>>)uncastHandle;
            
            if(envelope.getErrorCode() == ErrorCode.ERR_NONE) {
                SyncResponse response = SyncResponse.parseFrom(envelope.getPayload());
                handle.completedSuccess(response.getChosenList());
            } else {
                handle.completedError(envelope.getErrorCode());
            }
        } catch(InvalidProtocolBufferException e) {
            parent.logError("RPC client got bad packet from " + from);
            return; // ignore bad packets
        }
    }
    
    public void receiveDataResponse(Integer from, RPCEnvelope envelope) {
        try {
            Handle uncastHandle = ongoingCalls.remove(envelope.getCallID());
            if(uncastHandle == null) {
                parent.logError("RPC client got invalid callID from " + from);
                return;
            }
            Handle<DataResponse> handle = (Handle<DataResponse>)uncastHandle;
            
            if(envelope.getErrorCode() == ErrorCode.ERR_NONE) {
                DataResponse response = DataResponse.parseFrom(envelope.getPayload());
                handle.completedSuccess(response);
            } else {
                handle.completedError(envelope.getErrorCode());
            }
        } catch(InvalidProtocolBufferException e) {
            parent.logError("RPC client got bad packet from " + from);
            return; // ignore bad packets
        }
    }

    public void receiveCommitResponse(Integer from, RPCEnvelope envelope) {
        try {
            Handle uncastHandle = ongoingCalls.remove(envelope.getCallID());
            if(uncastHandle == null) {
                parent.logError("RPC client got invalid callID from " + from);
                return;
            }
            Handle<CommitResponse> handle = (Handle<CommitResponse>)uncastHandle;
            
            if(envelope.getErrorCode() == ErrorCode.ERR_NONE) {
                CommitResponse response = CommitResponse.parseFrom(envelope.getPayload());
                handle.completedSuccess(response); 
            } else {
                handle.completedError(envelope.getErrorCode());
            }
            
            // we just pass back the failed files on error -- Just have the user invalidate the
            // files in the transaction log. It's only a minor optimization to only invalidate the failed files,
            // since most transactions will be fairly small.
        } catch(InvalidProtocolBufferException e) {
            parent.logError("RPC client got bad packet from " + from);
            return; // ignore bad packets
        }
    }
    
    
    private RPCEnvelope newEnvelope(RPCEnvelope.Type type, Message payload) {
        return RPCEnvelope.newBuilder()
            .setType(type)
            .setErrorCode(ErrorCode.ERR_NONE)
            .setCallID(nextCallID++)
            .setPayload(payload.toByteString())
            .build();
    }
    
    private boolean sendEnvelope(int dest, RPCEnvelope envelope) {
        byte[] envelopeBytes = envelope.toByteArray();
        if(envelopeBytes.length > RIOPacket.MAX_PAYLOAD_SIZE)
            return false;
        parent.RIOSend(dest, Protocol.RPC, envelopeBytes);
        return true;
    }

    public void onCallFailure(int callID) {
        ongoingCalls.remove(callID).completedError(ErrorCode.ERR_SERVICE_FAILURE);
    }
}
