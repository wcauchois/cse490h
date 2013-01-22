package paxos;

import java.util.*;
import paxos.Messages.Promise;
import edu.washington.cs.cse490h.lib.Node;

/**
 * When running Multi-Paxos, we need to keep one Proposer for each instance of Paxos.
 * This class keeps track of all the individual Proposer.
 * 
 *
 */
public class AggregateProposer extends Role {
    // pxid -> proposer individual
    private Map<Integer, Proposer> proposers = new HashMap<Integer, Proposer>();
    
    /**
     * Return the learner for the given pxId
     * 
     * @param pxId
     * @return the learner, as above
     */
    public Proposer getIndividual(int pxId) {
        if(proposers.containsKey(pxId)) {
            return proposers.get(pxId);
        } else {
            Proposer individual = new Proposer(pxId, this.n,
                    new AggregateServices(services, pxId));
            proposers.put(pxId, individual);
            return individual;
        }
    }
    
    public AggregateProposer(Node node, Services services) {
        super(node, services);
    }
    
    /**
     * Demux the promise to the corresponding Proposer
     * 
     * @param pxId
     * @param from
     * @param promise
     */
    public void receivePromise(int pxId, int from, Promise promise) {
        Proposer individual = getIndividual(pxId);
        individual.receivePromise(from, promise);
    }
    
    @Override
    public void dumpInfo() {
        // dump the info for the latest and greatest acceptor
        // XXX: should we dump info for /all/ proposers?
        if(!proposers.keySet().isEmpty()) {
            Integer key = Collections.max(proposers.keySet());
            proposers.get(key).dumpInfo();
        }
    }
}
