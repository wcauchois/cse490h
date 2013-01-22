package debug;

import java.util.*;

import com.google.protobuf.InvalidProtocolBufferException;

import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataRequest;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.InvalidateRequest;
import proto.RPCProtos.RPCEnvelope;
import main.ErrorCode;
import main.Protocol;
import main.RIOPacket;

public class RIODisplayer extends PacketDisplayer {
    // configuration options
    private boolean showControlPackets = true; // "nocontrol"
    private boolean showInstanceIDs = false; // "showids"
    private boolean showSeqNums = true; // "noseqnums"
    
    private boolean noACK = false;
    private boolean noSYN = false;
    private boolean noPSH = false;
    
    @Override
    protected boolean onConfigLine(String line, int lineNo) {
        if(line.equals("nocontrol"))
            showControlPackets = false;
        else if(line.equals("showids"))
            showInstanceIDs = true;
        else if(line.equals("noseqnums"))
            showSeqNums = false;
        else if(line.equals("noACK"))
            noACK = true;
        else if(line.equals("noSYN"))
            noSYN = true;
        else if(line.equals("noPSH"))
            noPSH = true;
        return true;
    }
    
    public RIODisplayer() {
        super(Protocol.RIO, "rio");
    }
    
    @Override
    protected boolean shouldDisplay(int source, int dest, byte[] message) {
        RIOPacket pkt = RIOPacket.unpack(message);
        // don't show control packets if the user doesn't want to see them!
        boolean display = pkt.getProtocol() != -1 || showControlPackets;
        display = display && (!noACK || !pkt.isACK());
        display = display && (!noSYN || !pkt.isSYN());
        display = display && (!noPSH || !pkt.isPSH());
        return display;
    }
    
    @Override
    public String displayPacket(int source, int dest, byte[] message) {
        StringBuffer buf = new StringBuffer();
        RIOPacket pkt = RIOPacket.unpack(message);
        
        buf.append(genPrologue(source, dest));
        if(showSeqNums)
            buf.append("#" + pkt.getSeqNum() + " ");
        int rioProtocol = pkt.getProtocol();
        if(isControlPacket(pkt)) {
            buf.append("CONTROL");
            buf.append(flagsToString(pkt));
            buf.append(instanceIDsToString(pkt));
        } else {
            switch(rioProtocol) {
            case Protocol.SIMPLESEND_PKT:
                buf.append("SIMPLESEND_PKT");
                buf.append(instanceIDsToString(pkt));
                break;
            case Protocol.CACHETEST_PACKET:
                buf.append("CACHETEST_PACKET");
                buf.append(instanceIDsToString(pkt));
                break;
            case Protocol.RPC:
                buf.append("RPC");
                buf.append(instanceIDsToString(pkt));
                RPCEnvelope envelope = null;
                try {
                    envelope = RPCEnvelope.parseFrom(pkt.getPayload());
                } catch (InvalidProtocolBufferException e) {}
                buf.append(RPCDisplayer.parsePacket(envelope));
                break;
            default:
                buf.append("UNKNOWN(" + rioProtocol + ")");
                buf.append(flagsToString(pkt));
                buf.append(instanceIDsToString(pkt));
                break;
            }
        }
        
        return buf.toString();
    }
    private boolean isControlPacket(RIOPacket pkt) {
        return pkt.isACK() || pkt.isPSH() || pkt.isSYN() || pkt.isRST();
    }
    private String flagsToString(RIOPacket pkt) {
        List<String> flags = new ArrayList<String>();
        if(pkt.isACK()) flags.add("ACK");
        if(pkt.isPSH()) flags.add("PSH");
        if(pkt.isRST()) flags.add("RST");
        if(pkt.isSYN()) flags.add("SYN");
        return Arrays.toString(flags.toArray());
    }
    private String instanceIDsToString(RIOPacket pkt) {
        if(showInstanceIDs)
            return "{sourceID=" + pkt.getSourceID() + ";destID=" + pkt.getDestID() + "}";
        else
            return "";
    }
    
    // TODO: should there be a RPCDisplayer class in a separate file, with a separate whitelist / config parsing?
    private static class RPCDisplayer {
        public static String parsePacket(RPCEnvelope envelope) {
            StringBuilder result = new StringBuilder();
            
            result.append(" err: " + ErrorCode.codeToString(envelope.getErrorCode()));
            if(envelope.hasPayload()) {
                result.append(", ");
                result.append(parseEnvelopePayload(envelope));
            }
            return result.toString();
        }
        
        // pre: envelope.hasPayload()
        public static String parseEnvelopePayload(RPCEnvelope envelope) {
            try {
                switch(envelope.getType()) {
                case DataRequest:        return "DataRequest: " + DataRequest.parseFrom(envelope.getPayload()).toString().replace("\n", " ").replace("\\s+", " ");
                case DataResponse:       return "DataRespinse: " + DataResponse.parseFrom(envelope.getPayload()).toString().replace("\n", " ").replace("\\s+", " ");
                case CommitAttempt:      return "CommitAttempt: " + CommitAttempt.parseFrom(envelope.getPayload()).toString().replace("\n", " ").replace("\\s+", " ");
                case CommitResponse:     return "CommitResponse: " + CommitResponse.parseFrom(envelope.getPayload()).toString().replace("\n", " ").replace("\\s+", " ");
                case InvalidateRequest:  return "InvalidateRequest: " + InvalidateRequest.parseFrom(envelope.getPayload()).toString().replace("\n", " ").replace("\\s+", " ");
                default:                 return "";
                }
            } catch (Exception e){
                return "";
            }
        }
    }
}
