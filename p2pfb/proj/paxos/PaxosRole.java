package paxos;

import com.google.protobuf.AbstractMessage;

import edu.washington.cs.cse490h.lib.Node;

/**
 * When running Multi-Paxos, we need a Role associated with /each/ pxId
 */
public abstract class PaxosRole extends Role {
    private int pxId;
    
    protected int getPxId() {
        return pxId;
    }
    
    public PaxosRole(int pxId, Node node, Services services) {
        super(node, services);
        this.pxId = pxId;
    }
    
    @Override
    protected void send(int destAddr, AbstractMessage message) {
        // automatically attach out paxos id to outgoing messages!
        send(pxId, destAddr, message);
    }
}
