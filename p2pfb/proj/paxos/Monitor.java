package paxos;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import paxos.Messages.Heartbeat;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Node;

// How does the client figure out where to send requests?
//    He starts at address 0 (who should always be acting as the leader, if he is up)
//    If 0 doesn't respond within a timeout, the client sends to 1
//        If 1 is down, the client times out again and sends to 2, etc.
//        If 1 is currently acting as the leader, 1 will execute the request
//        If 1 is not the leader (0 was just not responsive), he tells the client to go to 0


// Thoughts:
//    - Should monitors be specified in the paxos.conf file, or should they be implicit?
//    - For our "production" system, we should have all nodes run all roles
//    - Make printing the contents of packets easier (Bill: what? isn't that what i did? :)
//    - Implement becomeLeader() in Proposer
//    - Implement relinquishLeader() in Proposer

/**
 * A monitor tracks who is the currently leader. This is accomplished by sending out heartbeats.
 * Note that each paxos node runs their own monitor, so each node independantly decides on who it 
 * thinks the current leader is. As a result, it's possible that there will be multiple leaders at
 * one time. Safety is ensured though, and as soon as a leader recieves a message from a paxos
 * node with a lower address, it relinquishes its leadership
 */
public class Monitor extends Role {
    private static final int HEARTBEAT_THRESHOLD = 4;
    private static final int HEARTBEAT_TIMEOUT = 4;
    // node address -> # consecutive heartbeat failures
    private Map<Integer, Integer> failedHeartbeatCount = new HashMap<Integer, Integer>();
    // the addresses of the nodes which succesfully returned heartbeats this round
    private SortedSet<Integer> heartbeatsReceived = new TreeSet<Integer>();
    // for heartbeat "rounds"
    private Method onTimeoutMethod;

    /**
     * ctor
     * 
     * @param node a reference to the node acting this role
     * @param services services provided to this role
     */
    public Monitor(Node node, Services services) {
        super(node, services);
        try {
            onTimeoutMethod = getClass().getMethod("onTimeout");
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1); // FML
        }
        
        for(Integer paxosNode : services.getPaxosAddrs())
            if(paxosNode != n.addr) // don't send heartbeats to ourself
                failedHeartbeatCount.put(paxosNode, 0);
        
        // start beating our heart
        addTimeout();
    }
    
    /**
     * Called when a timeout occurs
     */
    public void onTimeout() {
        for(Integer node : failedHeartbeatCount.keySet()) {
            if(!heartbeatsReceived.contains(node)) {
                failedHeartbeatCount.put(node, failedHeartbeatCount.get(node) + 1);
            } else {
                failedHeartbeatCount.put(node, 0);
            }
        }
        heartbeatsReceived.clear();
        
        transactions.Server server = services.getRole(transactions.Server.class);
        if(weAreLeader()) // we're the leader!
            server.ensureLeader(); // idempotent
        
        sendHeartbeats();
        
        addTimeout();
    }
    
    private void addTimeout() {
        n.addTimeout(new Callback(onTimeoutMethod, this, new Object[0]), HEARTBEAT_TIMEOUT);
    }
    
    /**
     * @return Whether we are currently the leader
     */
    public boolean weAreLeader() {
        return getLeader() == n.addr;
    }
    
    /**
     * @return the address of the node we think the leader is
     */
    public int getLeader() {
        return getAliveNodes().first();
    }
    
    private SortedSet<Integer> getAliveNodes() {
        // HEARTBEAT_THRESHOLD packets must be dropped before we consider a node down
        SortedSet<Integer> upNodes = new TreeSet<Integer>();
        for(Map.Entry<Integer, Integer> pair : failedHeartbeatCount.entrySet())
            if(pair.getValue() <= HEARTBEAT_THRESHOLD)
                upNodes.add(pair.getKey());
        
        upNodes.add(n.addr); // we know we're alive!!
        
        return upNodes;
    }
    
    // This is a "push" model -- all nodes always send out heartbeats
    // TODO: can we optimize this to only send heartbeats from the leader?
    //       N^2 messages every three rounds is pretty heavy....
    private void sendHeartbeats() {
        for(Integer paxosNode : failedHeartbeatCount.keySet()) {
            send(paxosNode, Heartbeat.newBuilder().build());
        }
    }
    
    /**
     * Receive a Heartbeat message from a peer
     * 
     * @param from the address of our peer
     * @param beat the message
     */
    public void receiveHeartbeat(int from, Heartbeat beat) {
        heartbeatsReceived.add(from);
    }
    
    @Override
    public void dumpInfo() {
        services.logPaxos(this, "info: ");
        services.logPaxos(this, "      failedHeartbeatCount " + failedHeartbeatCount);
        services.logPaxos(this, "      heartbeatsReceived " + heartbeatsReceived);
        services.logPaxos(this, "      current leader is " + getLeader());
    }
}
