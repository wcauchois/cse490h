package main;
import static proto.RPCProtos.*;

import java.util.List;

import primitives.Handle;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.RPCEnvelope;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * The set of server RPC stubs
 */
public class RPCServer {
    private RPCNode parent;
    
    /**
     * Constructor
     * @param parent our parent node.
     */
    public RPCServer(RPCNode parent) {
        this.parent = parent;
    }
    
    /**
     * Unpack the invalidateRequest() packet and call the handleinvalidateRequest() implementation. On
     * completion of handleinvalidateRequest(), send a InvalidationConfirm packet to the callee
     * 
     * @param from where the RPC came from
     * @param envelope the raw packet
     */
    public void receiveInvalidateRequest(Integer from, RPCEnvelope envelope) {
        try {
            InvalidateRequest ir = InvalidateRequest.parseFrom(envelope.getPayload());
            parent.handleInvalidateRequest(from, ir.getFilesList());
        } catch (InvalidProtocolBufferException e) {
            parent.logError("RPC server got  bad packet from " + from);
            return;
        }
    }
    
    public void receiveDataRequest(final Integer from, final RPCEnvelope envelope) {
        try {
            final DataRequest dataRequest = DataRequest.parseFrom(envelope.getPayload());
            Handle<DataResponse> result = parent.handleDataRequest(from, dataRequest.getFilename());
            
            result.addListener(new Handle.Listener<DataResponse>(){
                public void onError(final int errorCode) {
                    RPCEnvelope reply = newErrorEnvelope(errorCode, envelope.getCallID(), RPCEnvelope.Type.DataResponse);

                    parent.RIOSend(from, Protocol.RPC, reply.toByteArray());                                            
                }
                
                public void onSuccess(final DataResponse response) {
                    RPCEnvelope reply = newDataEnvelope(envelope.getCallID(), RPCEnvelope.Type.DataResponse, response);
                    
                    parent.RIOSend(from, Protocol.RPC, reply.toByteArray());
                }
            });
        } catch (InvalidProtocolBufferException e) {
            parent.logError("RPC server got  bad packet from " + from);
            return;
        }        
    }

    public void receiveCommitAttempt(final Integer from, final RPCEnvelope envelope) {
        try {
            final CommitAttempt commitAttempt = CommitAttempt.parseFrom(envelope.getPayload());
            Handle<CommitResponse> result = parent.handleCommitAttempt(from, commitAttempt);
            
            result.addListener(new Handle.Listener<CommitResponse>(){
                public void onError(final int errorCode) {
                    RPCEnvelope reply = newErrorEnvelope(errorCode, envelope.getCallID(), RPCEnvelope.Type.CommitResponse);
                    
                    parent.RIOSend(from, Protocol.RPC, reply.toByteArray());
                }
                
                public void onSuccess(final CommitResponse response) {
                    RPCEnvelope reply = newDataEnvelope(envelope.getCallID(), RPCEnvelope.Type.CommitResponse, response);
                    
                    parent.RIOSend(from, Protocol.RPC, reply.toByteArray());
                }
            });
        } catch (InvalidProtocolBufferException e) {
            parent.logError("RPC server got  bad packet from " + from);
            return;
        }
    }
    
    public void receiveSyncRequest(final Integer from, final RPCEnvelope envelope) {
        List<ChosenTransaction> chosenTransactions = parent.handleSyncRequest();
        
        SyncResponse response = SyncResponse.newBuilder()
            .addAllChosen(chosenTransactions)
            .build();
        
        RPCEnvelope reply = newDataEnvelope(envelope.getCallID(), RPCEnvelope.Type.SyncResponse, response);
        
        parent.RIOSend(from, Protocol.RPC, reply.toByteArray());
    }

    private RPCEnvelope newErrorEnvelope(int errorCode, int callID, RPCEnvelope.Type type) {
      return RPCEnvelope.newBuilder()
            .setType(type)
            .setCallID(callID)
            .setErrorCode(errorCode)
            .build();
    }
    
    private RPCEnvelope newDataEnvelope(int callID, RPCEnvelope.Type type, Message payload) {
        RPCEnvelope.Builder builder = RPCEnvelope.newBuilder()
            .setType(type)
            .setCallID(callID)
            .setErrorCode(ErrorCode.ERR_NONE);
        if(payload != null)
            builder.setPayload(payload.toByteString());
        return builder.build();
    }
}
