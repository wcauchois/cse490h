package paxos;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Node;

import java.lang.reflect.Method;
import java.util.*;

import paxos.Messages.AcceptedProposal;
import proto.RPCProtos.CommitAttempt;

/**
 * When running Multi-Paxos, we need to keep one Learner for each instance of Paxos.
 * This class keeps track of all the individual Learners.
 * 
 * The AggregateLearner is responsible for filling in gaps in the replicated state-machine's
 * committed transactions
 *
 */
public class AggregateLearner extends Role {
    // for learning unchosen paxos IDs
    private static final int REPROPOSE_TIMEOUT = 10;
    private Method onTimeoutMethod;

    // pxid -> learner
    private Map<Integer, Learner> learners = new HashMap<Integer, Learner>();
       
    /**
     * Return the learner for the given pxId
     * 
     * @param pxId
     * @return the learner, as above
     */
    public Learner getIndividual(int pxId) {
        if(learners.containsKey(pxId)) {
            return learners.get(pxId);
        } else {
            Learner individual = new Learner(pxId, this.n,
                    new AggregateServices(services, pxId));
            learners.put(pxId, individual);
            return individual;
        }
    }

    public AggregateLearner(Node node, Services services) {
        super(node, services);
        
        try {
            onTimeoutMethod = getClass().getMethod("onTimeout");
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1); // FML
        }
        
        addTimeout();
    }
    
    /**
     * Demux the message to the corresponding Learner
     * 
     * @param pxId
     * @param from
     * @param message
     */
    public void receiveAcceptedProposal(int pxId, int from, AcceptedProposal message) {
        Learner individual = getIndividual(pxId);
        individual.receiveAcceptedProposal(from, message);
    }

    @Override
    public void dumpInfo() {
        if(!learners.keySet().isEmpty()) {
            // dump the info for the latest and greatest learner
            Integer key = Collections.max(learners.keySet());
            learners.get(key).dumpInfo();
        }
    }
    
    // make sure to fill in gaps in our learned values... we do this by attempting to propose the
    // pending value known by the server, or NOP
    public void onTimeout() {
        paxos.Monitor monitor = services.getRole(paxos.Monitor.class);
        if(monitor == null || !monitor.weAreLeader())
            return;
        
        transactions.Server server = services.getRole(transactions.Server.class);
        if(server == null)
            return;
        
        paxos.AggregateProposer aggregateProposer = services.getRole(paxos.AggregateProposer.class);
        if(aggregateProposer == null)
            return;
        
        Map<Integer, CommitAttempt> unchosenPaxosValues = server.getUnchosenPaxosIds();
        for(Map.Entry<Integer, CommitAttempt> entry : unchosenPaxosValues.entrySet()) {
            paxos.Proposer proposer = aggregateProposer.getIndividual(entry.getKey());
            if(proposer != null) {
                proposer.propose(entry.getValue());
            } // else... wtf?
        }
        
        addTimeout();
    }
    
    private void addTimeout() {
        n.addTimeout(new Callback(onTimeoutMethod, this, new Object[0]), REPROPOSE_TIMEOUT);
    }
}
