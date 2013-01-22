package paxos;

import java.util.Set;

import proto.RPCProtos.CommitAttempt;

public class Util {
    public static final int NOP_TXID = -1;
    public static final CommitAttempt NOP = CommitAttempt.newBuilder().setFrom(-1).setId(NOP_TXID).build();
    
    /**
     * @param <E> what we're comparing
     * @param subset 
     * @param superset
     * @return whether size(subset) > size(superset) / 2
     */
    public static <E> boolean isMajorityOf(Set<E> subset, Set<E> superset) {
        return isMajorityOf(subset.size(), superset.size());
    }
    
    /**
     * 
     * @param count
     * @param totalCount
     * @return whether count > totalCount / 2
     */
    public static boolean isMajorityOf(int count, int totalCount) {
        return (count > totalCount / 2);
    }
}