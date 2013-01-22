package debug;

import com.google.protobuf.InvalidProtocolBufferException;

import main.Protocol;
import paxos.Messages.*;
import java.util.Set;
import java.util.TreeSet;

public class PaxosDisplayer extends PacketDisplayer {
    private Set<Type> typeWhitelist = new TreeSet<Type>();
    
    public PaxosDisplayer() {
        super(Protocol.PAXOS, "paxos");
    }
    
    private String proposalToString(Proposal p) {
        return "[" + p.getN() + ", " + p.getV().toString().replace('\n', ' ') + "]";
    }

    @Override
    protected boolean shouldDisplay(int source, int dest, byte[] message) {
        try {
            Envelope env = Envelope.parseFrom(message);
            return typeWhitelist.isEmpty() || typeWhitelist.contains(env.getType());
        } catch(InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return true;
    }
    
    @Override
    protected boolean onConfigLine(String line, int lineNo) {
        if(line.equals("accept_request"))
            typeWhitelist.add(Type.ACCEPT_REQUEST);
        else if(line.equals("accepted_proposal"))
            typeWhitelist.add(Type.ACCEPTED_PROPOSAL);
        else if(line.equals("heartbeat"))
            typeWhitelist.add(Type.HEARTBEAT);
        else if(line.equals("prepare_request"))
            typeWhitelist.add(Type.PREPARE_REQUEST);
        else if(line.equals("promise"))
            typeWhitelist.add(Type.PROMISE);
        else {
            System.err.println("unrecognized paxos message type: " + line);
            return false; // bad parse
        }
        return true;
    }
    
    @Override
    public String displayPacket(int source, int dest, byte[] message) {
        try {
            Envelope env = Envelope.parseFrom(message);
            StringBuffer buf = new StringBuffer();
            String prologue = "Packet: " + + source + "->" + dest + " protocol: PAXOS";
            if(env.hasPxId())
                prologue += "(" + env.getPxId() + ")";
            prologue += " contents: ";
            buf.append(prologue);
            switch(env.getType()) {
            case ACCEPT_REQUEST:
                AcceptRequest acceptRequest = AcceptRequest.parseFrom(env.getPayload());
                buf.append("AcceptRequest(" + proposalToString(acceptRequest.getProposal()) + ")");
                break;
            case ACCEPTED_PROPOSAL:
                AcceptedProposal acceptedProposal = AcceptedProposal.parseFrom(env.getPayload());
                buf.append("AcceptedProposal(" + proposalToString(acceptedProposal.getProposal()) + ")");
                break;
            case HEARTBEAT:
                buf.append("Heartbeat");
                break;
            case PREPARE_REQUEST:
                PrepareRequest prepareRequest = PrepareRequest.parseFrom(env.getPayload());
                buf.append("PrepareRequest(" + prepareRequest.getN() + ")");
                break;
            case PROMISE:
                Promise promise = Promise.parseFrom(env.getPayload());
                buf.append("Promise(prepareN: " + promise.getPrepareN());
                if(promise.hasAcceptedPreviously()) {
                    buf.append(", acceptedPreviously: " + proposalToString(promise.getAcceptedPreviously()));
                }
                buf.append(")");
                break;
            }
            return buf.toString();
        } catch(InvalidProtocolBufferException e) {
            e.printStackTrace();
            return "ERROR";
        }
    }
}
