package paxos;

import java.util.*;

import paxos.Messages.AcceptRequest;
import paxos.Messages.PrepareRequest;
import paxos.Messages.Promise;
import edu.washington.cs.cse490h.lib.Node;

/**
 * When running Multi-Paxos, we need to keep one Acceptor for each instance of Paxos.
 * This class keeps track of all the individual Acceptors
 *
 */
public class AggregateAcceptor extends Role {
    // pxid -> Acceptor
    private Map<Integer, Acceptor> acceptors = new HashMap<Integer, Acceptor>();
    
    /**
     * Return the acceptor for the given pxId
     * 
     * @param pxId
     * @return the acceptor, as above
     */
    public Acceptor getIndividual(int pxId) {
        if(acceptors.containsKey(pxId)) {
            return acceptors.get(pxId);
        } else {
            Acceptor individual = new Acceptor(pxId, this.n,
                    new AggregateServices(services, pxId));
            acceptors.put(pxId, individual);
            return individual;
        }
    }
    
    public AggregateAcceptor(Node node, Services services) {
        super(node, services);
    }
    
    /**
     * demux the given prepare request to the Acceptor for the pxId
     * 
     * @param pxId
     * @return the Promise, if any, returned from the Acceptor
     */
    public Promise receivePrepareRequest(int pxId, int from, PrepareRequest request) {
        Acceptor individual = getIndividual(pxId);
        return individual.receivePrepareRequest(from, request);
    }
    
    /**
     * demux the given Accept request to the Acceptor for the pxId
     * 
     * @param pxId
     */
    public void receiveAcceptRequest(int pxId, int from, AcceptRequest request) {
        Acceptor individual = getIndividual(pxId);
        individual.receiveAcceptRequest(from, request);
    }

    @Override
    public void dumpInfo() {
        // dump the info for the latest and greatest acceptor
        if(!acceptors.keySet().isEmpty()) {
            Integer key = Collections.max(acceptors.keySet());
            acceptors.get(key).dumpInfo();
        }
    }
}
