package paxos;

import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Node;
import paxos.Messages.AcceptRequest;
import paxos.Messages.PrepareRequest;
import paxos.Messages.Promise;
import paxos.Messages.Proposal;
import proto.RPCProtos.CommitAttempt;

/**
 * A proposer sends a proposed value to a set of acceptors
 */
public class Proposer extends PaxosRole {
    private static final String PROPOSAL_NUMBER = "PAXOS_proposal_num.txt";
    
    private String getProposalNumberFilename() {
        return getPxId() + "_" + PROPOSAL_NUMBER;
    }
    
    // The Proposer is implemented as the following state machine:
    // 
    //               ___________  promise received from majority      ____________
    //  propose() -> | Prepare | -------------------------------->    | Propose  |
    //            '- -----------          propose()                   ------------
    //                      ^------------------------------------------
    private State currentState; 
    
    // algorithm for choosing a proposal number:
    //     start with this node's current address, and increment by the
    //     number of nodes in the system. this way, each node is guaranteed
    //     to have a unique increasing sequence of proposal numbers!
    private int currentProposalNumber;
    
    /**
     * ctor
     * 
     * @param node a reference to the node acting this role
     * @param services services provided to this role
     */
    public Proposer(int pxId, Node node, Services services) {
        super(pxId, node, services);
        Integer nextProposalNumber = services.getFileSystem().getID(getProposalNumberFilename());
        currentProposalNumber = (nextProposalNumber == null) ? node.addr : nextProposalNumber;
    }
    
    // Represents a State in the Proper's DFA
    private abstract class State { }
    
    // If we are currently in the prepare state, we need to know the proposal number and value we are trying to propose,
    // and the set of promises we have received
    private class PrepareState extends State {
        public int n;
        public CommitAttempt v; // the original proposal number and value we were trying to propose
        // node address -> promise
        public Map<Integer, Promise> receivedPromises = new HashMap<Integer, Promise>();
        public PrepareState(int n, CommitAttempt v) {
            this.n = n;
            this.v = v;
        }
        
        @Override
        public String toString() {
            return "PrepareState: n="+n+",v="+v+", receivedPromises:"+receivedPromises;
        }
    }
    
    // If we are in the propose state, we need to know...
    private class ProposeState extends State {
        @Override
        public String toString() {
            return "ProposeState";
        }
    }
    
    // Make a state transition
    private void setState(State state) {
        this.currentState = state;
    }
    
    // Get the data associated with the current prepare state, or throw an IllegalStateException
    // if we are not currently in the PrepareState
    private PrepareState prepareState() {
        if(currentState instanceof PrepareState)
            return (PrepareState)currentState;
        else
            throw new IllegalStateException(n.addr + " is not in a prepare state!");
    }
    
    /**
     * Have the proposer attempt to propose the given value v.
     * 
     * @param v the value to propose
     */
    public void propose(CommitAttempt v) {
        if(!services.getRole(paxos.Monitor.class).weAreLeader()) // hmmmm, this makes testing quite a bit more difficult...
           throw new IllegalStateException("Non-leaders shouldn't be proposing values!");
            
        
        int n = currentProposalNumber;
        
        this.n.logSynopticEvent("PROPOSER:NEW-PROPOSE["+n+"]");

        services.getFileSystem().updateID(getProposalNumberFilename(), currentProposalNumber += services.getNodeCount());
        setState(new PrepareState(n, v));
        services.logPaxos(this, "proposing (" + n + ", " + v.toString().replace('\n', ' ') + ")");
        // send a prepare request to "a majority of acceptors" (in this case all of them)
        for(Integer acceptor : services.getAcceptorAddrs()) {
            send(acceptor, PrepareRequest.newBuilder().setN(n).build());
        }
    }
    
    /**
     * Receive a promise from an acceptor
     * 
     * @param from
     * @param promise
     */
    public void receivePromise(int from, Promise promise) {
        this.n.logSynopticEvent("PROPOSER:RECV-PROMISE["+promise.getPrepareN()+"]");

        // If the promise does not correspond to our current propsosal, ingore it
        if(!(currentState instanceof PrepareState)) {
            // this means we received a promise after we've already sent all our AcceptRequests
            // XXX: should we send an AcceptRequest to the acceptor that just gave us a promise??
            return;
        }
        if(promise.getPrepareN() != prepareState().n) {
            return;
        }
        
        Map<Integer, Promise> receivedPromises = prepareState().receivedPromises;
        receivedPromises.put(from, promise);
        
        if(Util.isMajorityOf(receivedPromises.keySet(), services.getAcceptorAddrs())) {
            // we have received a promise from a majority of acceptors; that means
            // we're ready to proceed with our proposal

            int highestN = -1;
            CommitAttempt highestV = null;
            for(Promise receivedPromise : receivedPromises.values()) {
                if(receivedPromise.hasAcceptedPreviously()) {
                    Proposal proposal = receivedPromise.getAcceptedPreviously();
                    if(proposal.getN() > highestN) {
                        highestN = proposal.getN();
                        highestV = proposal.getV();
                    }
                }
            }
            // if no other value has been chosen, we get to choose whatever we want. Else we must choose the highest
            // chosen value
            CommitAttempt v = (highestN < 0) ? prepareState().v : highestV;
            int n = prepareState().n;
            // send a proposal numbered n with a value v, where v is the value of
            // the highest numbered proposal among the responses, or is any value
            // if the responses reported no proposals
            AcceptRequest request = AcceptRequest.newBuilder().setProposal(
                        Proposal.newBuilder().setN(n).setV(v).build()
                    ).build();
            // submit the accept request to each acceptor that responded
            for(Integer acceptor : receivedPromises.keySet())
                send(acceptor, request);
            setState(new ProposeState()); // TODO: transition from propose state back to prepare state????
        } else {
            // wait until we get more promises!
            // XXX: timeouts?
            // Do we really need timeouts? If the user gets tired of waiting on their latest proposal to go through,
            // they can just call propose(n, v) again, right?
        }
    }
    
    @Override
    public void dumpInfo() {
        services.logPaxos(this, "info:");
        services.logPaxos(this, "    currentState: " + currentState);
        services.logPaxos(this, "    currentProposalNumber: " + currentProposalNumber);
    }
}
