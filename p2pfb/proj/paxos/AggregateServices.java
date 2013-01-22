package paxos;

import java.util.Set;
import java.util.SortedSet;

import main.FSUtil;

/**
 * Services tied to particular paxos instance
 *
 */
public class AggregateServices implements Services {
    private int pxId;
    private Services services;
    
    public AggregateServices(Services services, int pxId) {
        this.services = services;
        this.pxId = pxId;
    }
    
    @Override
    public <T extends Role> T getRole(Class<T> klass) {
        return getIndividualRole(this.services, klass, this.pxId);
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends Role> T getIndividualRole(Services services, Class<T> individualKlass, int pxId) {
        if(individualKlass.equals(Proposer.class)) {
            AggregateProposer aggregate = services.getRole(AggregateProposer.class);
            return (T)aggregate.getIndividual(pxId);
        } else if(individualKlass.equals(Learner.class)) {
            AggregateLearner aggregate = services.getRole(AggregateLearner.class);
            return (T)aggregate.getIndividual(pxId);
        } else if(individualKlass.equals(Acceptor.class)) {
            AggregateAcceptor aggregate = services.getRole(AggregateAcceptor.class);
            return (T)aggregate.getIndividual(pxId);
        } else {
            return services.getRole(individualKlass);
        }
    }

    @Override
    public void logPaxos(Role role, String message) {
        services.logPaxos(role, "(" + pxId + ") " + message);
    }
    
    //////////////////////////////////////////////////////////////////////
    public Set<Integer> getAcceptorAddrs()
        { return services.getAcceptorAddrs(); }
    public FSUtil getFileSystem()
        { return services.getFileSystem(); }
    public Set<Integer> getLearnerAddrs()
        { return services.getLearnerAddrs(); }
    public int getNodeCount()
        { return services.getNodeCount(); }
    public SortedSet<Integer> getPaxosAddrs()
        { return services.getPaxosAddrs(); }
}