package paxos;

import java.util.*;

import paxos.Messages.AcceptedProposal;
import paxos.Messages.Proposal;
import edu.washington.cs.cse490h.lib.Node;

/**
 * A Learner simply listens to the different proposals accepted by the acceptors. When a learner
 * discovers that a proposal has been chosen by a majority of acceptors, we are done, since the
 * value can no longer change.
 */
public class Learner extends PaxosRole {
    // acceptor address -> proposal
    // there is no need to persist this data to disc, since we can just hav the proposers propose a new
    // value if the learner crashes and consequently would like to know what value was chosen
    private Map<Integer, Proposal> acceptedProposals = new HashMap<Integer, Proposal>();
    
    /**
     * ctor
     * 
     * @param node a reference to the node acting this role
     * @param services services provided to this role
     */
    public Learner(int pxId, Node node, Services services) {
        super(pxId, node, services);
    }
    
    // Return a Map from Proposal -> # of nodes that have accepted it. Note that
    // we only track the latest accepted proposal from each node. This means that even if a
    // value has been chosen, there are pathological cases where a learner will never learn
    // the value; the acceptors may be constantly accepting new proposal /numbers/ such that
    // the learner never sees that a majority of nodes has accepted any particular proposal.
    // I wonder if there is a way around this... in any case, safety is not compromised, only liveness
    private Map<Proposal, Integer> getProposalCounts() {
        // XXX: does hashcode() on proposals work correctly?
        Map<Proposal, Integer> counts = new HashMap<Proposal, Integer>();
        for(Proposal proposal : acceptedProposals.values()) {
            if(counts.containsKey(proposal))
                counts.put(proposal, counts.get(proposal) + 1);
            else
                counts.put(proposal, 1);
        }
        return counts;
    }
    
    // Return the Proposal that has been chosen by a majority of acceptors, else null if
    // no such proposal exists
    private Proposal getChosenProposal() {
        Map<Proposal, Integer> counts = getProposalCounts();
        for(Map.Entry<Proposal, Integer> pair : counts.entrySet()) {
            if(Util.isMajorityOf(pair.getValue(), services.getAcceptorAddrs().size())) {
                return pair.getKey();
            }
        }
        return null; // no proposal has been chosen yet
    }
    
    // Whenever an acceptor accepts a proposal, it notifies all learners
    public void receiveAcceptedProposal(int from, AcceptedProposal message) {
        n.logSynopticEvent("LEARNER:RECV-ACCEPTED["+message.getProposal().getN()+"]");
        
        Proposal proposal = message.getProposal();
        // we only track the /most recent/ proposal from any given acceptor
        acceptedProposals.put(from, proposal);
        
        Proposal chosenProposal = getChosenProposal();
        if(chosenProposal != null) {
            n.logSynopticEvent("LEARNER:LEARNED-VALUE["+chosenProposal.getN()+"]");
            
            // TODO: notify whoever that a value has been chosen!!!!!
            services.logPaxos(this, "learned that (" + chosenProposal.getN()
                    + ", " + chosenProposal.getV().toString().replace('\n', ' ') + ") has been chosen");
            
            transactions.Server server = services.getRole(transactions.Server.class);
            if(server != null)
                server.onValueChosen(this.getPxId(), chosenProposal.getV());
        }
    }
    
    @Override
    public void dumpInfo() {
        services.logPaxos(this, "info:");
        services.logPaxos(this, "    acceptedProposals: " + acceptedProposals);
    }
}
