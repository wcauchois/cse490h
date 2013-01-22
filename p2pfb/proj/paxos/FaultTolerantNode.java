package paxos;

import primitives.Handle;
import proto.RPCProtos.ChosenTransaction;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


import main.FSUtil;
import main.Protocol;
import main.RPCNode;

/**
 * A test node for our paxos implementation
 */
public class FaultTolerantNode extends RPCNode implements Actor {
    private static int testPxId = 0;
    
    // the roles we are currently playing.
    private List<Role> myRoles = new ArrayList<Role>();
    private AggregateProposer proposerRole = null;
    private AggregateLearner learnerRole = null;
    private AggregateAcceptor acceptorRole = null;
    private Monitor monitorRole = null;
    private transactions.Client clientRole = null;
    private transactions.Server serverRole = null;
    private static TestConfig config = null;
    // a static list of all acceptors in the system
    private static Set<Integer> acceptorAddrs = new HashSet<Integer>();
    // a static list of all learners in the system
    private static Set<Integer> learnerAddrs = new HashSet<Integer>();
    // a static list of all paxos nodes in the system (excludes clients)
    private static SortedSet<Integer> paxosAddrs = new TreeSet<Integer>();
    // a static count of all nodes in the system
    private static int nodeCount = 0;
    
    private FSUtil fileSystem;
    
    public static double getFailureRate() { return 0/100.0; }
    public static double getDropRate() { return 0/100.0; }
    public static double getDelayRate() { return 0/100.0; }
    
    // The Services Object we pass to each of our Roles
    private class TestServices implements paxos.Services {
        @Override
        public Set<Integer> getAcceptorAddrs() {
            return Collections.unmodifiableSet(acceptorAddrs);
        }

        @Override
        public Set<Integer> getLearnerAddrs() {
            return Collections.unmodifiableSet(learnerAddrs);
        }
        
        @Override
        public SortedSet<Integer> getPaxosAddrs() {
            return Collections.unmodifiableSortedSet(paxosAddrs);
        }

        @Override
        public int getNodeCount() {
            return nodeCount;
        }
        
        @Override
        public void logPaxos(Role role, String message) {
            String roleString = "";
            if(role instanceof Proposer) roleString = "[proposer]";
            if(role instanceof Acceptor) roleString = "[acceptor]";
            if(role instanceof Learner) roleString = "[learner]";
            if(role instanceof Monitor) roleString = "[monitor]";
            if(role instanceof transactions.Server) roleString = "[txserver]";
            if(role instanceof transactions.Client) roleString = "[txclient]";
            log(roleString + " " + message);
        }
        
        @Override
        public FSUtil getFileSystem() {
            return fileSystem;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends Role> T getRole(Class<T> klass) {
            for(Role r : myRoles) {
                if(r.getClass().equals(klass))
                    return (T)r;
            }
            return null;
        }
    }
    
    // A config file loaded into memory on boot. Config files essentially just decide which
    // nodes should play which role.
    private class TestConfig {
        // node address -> list of roles
        private Map<Integer, Set<Messages.Role>> nodeRoles = new HashMap<Integer, Set<Messages.Role>>();
        public static final String FILE_NAME = "paxos.conf";
        // config format consists of node address followed by a colon, followed by a comma-delimited list
        // of the roles that the node at that address should assume.
        
        /*
         * example config:
         * 
         * 0: acceptor, proposer
         * 1: acceptor
         * 2: learner
         * 3: acceptor
         */
        public void load() throws IOException {
            Scanner sc = new Scanner(new File(FILE_NAME));
            while(sc.hasNext()) {
                String line = sc.nextLine();
                if(line.trim().length() == 0 || line.startsWith("#"))
                    continue;
                String[] parts = line.split(":");
                int addr = Integer.parseInt(parts[0]);
                String[] roleStrings = parts[1].trim().split(",");
                Set<Messages.Role> roles = new HashSet<Messages.Role>(roleStrings.length); 
                for(String roleString : roleStrings) {
                    String s = roleString.trim().toLowerCase();
                    if(s.equals("proposer")) {
                        roles.add(Messages.Role.PROPOSER);
                    } else if(s.equals("acceptor")) {
                        roles.add(Messages.Role.ACCEPTOR);
                    } else if(s.equals("learner")) {
                        roles.add(Messages.Role.LEARNER);
                    } else if(s.equals("monitor")) {
                        roles.add(Messages.Role.MONITOR);
                    } else if(s.equals("client")) {
                        roles.add(Messages.Role.TXCLIENT);
                    } else if(s.equals("server")) {
                        roles.add(Messages.Role.TXSERVER);
                    } else {
                        logError("unrecognized role: " + s);
                    }
                }
                nodeRoles.put(addr, roles);
            }
            logSuccess("loaded " + FILE_NAME);
        }
        
        // return the roles specified in the config file for the given node. returns null
        // if no such roles have been specified
        public Set<Messages.Role> getRoles(int addr) {
            return nodeRoles.get(addr);
        }
        
        /**
         * @return the number of nodes in the system
         */
        public int getNodeCount() {
            return nodeRoles.size();
        }
    }
    
    @Override
    public void onCommand(String command) {
        if(command.equals("bullshit")) { // XXX: remove for turnin
            RIOSend(1, Protocol.SIMPLESEND_PKT, new byte[0]);
            log("bullshit!!! lol");
            return;
        }
        try {
            String[] args = null;
            if(command.startsWith("propose")) {
                args = command.split(" ");
                int value = Integer.parseInt(args[1]);
                if(proposerRole == null) {
                    logError("i'm not a proposer!");
                } else {
                    CommitAttempt commit = CommitAttempt.newBuilder().setFrom(-1).setId(value).build();
                    proposerRole.getIndividual(testPxId).propose(commit);
                }
            } else if(command.startsWith("pxinc")) {
                testPxId++;
                log("the test Paxos id is now: " + testPxId);
            } else if(command.startsWith("info")) {
                dumpInfo();
            } else if(clientRole != null) {
                // Send everything else to the client
                clientRole.onCommand(command);
            } else {
                logError("Unknown command " + command);
            }
        } catch(IllegalArgumentException e) {
            // This also catches NumberFormatException from Integer.parseInt()
            logError("bad command syntax");
        }
    }

    @Override
    public void onReceive(Integer from, int protocol, byte[] msg) {
        if(protocol == Protocol.PAXOS) {
            Role.demux(this, msg, from);
        } else {
            super.onReceive(from, protocol, msg);
        }
    }
    
    // our static configuration state (e.g. paxosAddrs) needs to be initialized all at once, since nodes
    // may depend on that state in their constructors, and the order in which we call their
    // constructors is arbitrary
    private void ensureConfigIsLoaded() {
        if(config != null)
            return; // the config has already been loaded
        
        config = new TestConfig();
        try {
            config.load();
        } catch(IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        nodeCount = config.getNodeCount();
        
        for(Entry<Integer, Set<Messages.Role>> entry : config.nodeRoles.entrySet()) {
            Integer node = entry.getKey();
            Set<Messages.Role> roles = entry.getValue();
            
            // invariant: if we're a client, we can run no other role
            if(roles.contains(Messages.Role.TXCLIENT)) {
                if(roles.size() != 1) {
                    throw new RuntimeException("a client cannot run any other roles!");
                }
                roles.add(Messages.Role.TXCLIENT);
            } else {
                // else we're a paxos node
                paxosAddrs.add(node);
                
                if(roles.contains(Messages.Role.ACCEPTOR))
                    acceptorAddrs.add(node);
                if(roles.contains(Messages.Role.LEARNER))
                    learnerAddrs.add(node);
            }
        }
    }
    
    @Override
    public void start() {
        super.start();
        log("starting up...");
        fileSystem = new FSUtil(this);
        ensureConfigIsLoaded();
        
        // initialize the PacketDisplayer framework (note this method can be invoked multiple
        // time idempotently).
        debug.PacketDisplayer.initialize();
        
        TestServices services = new TestServices();
        Set<paxos.Messages.Role> roles = config.getRoles(this.addr);
        
        if(roles == null)
            return;
        
        for(Messages.Role role : roles) {
            switch(role) {
            case TXCLIENT: // we are guaranteed that a client will run no other roles
                log("becoming an client");
                clientRole = new transactions.Client(this, services);
                myRoles.add(clientRole);
                break;
            case ACCEPTOR:
                log("becoming an acceptor");
                acceptorRole = new paxos.AggregateAcceptor(this, services);
                myRoles.add(acceptorRole);
                break;
            case LEARNER:
                log("becoming a learner");
                learnerRole = new paxos.AggregateLearner(this, services);
                myRoles.add(learnerRole);
                break;
            case PROPOSER:
                log("becoming a proposer");
                proposerRole = new paxos.AggregateProposer(this, services);
                myRoles.add(proposerRole);
                break;
            }
        }
        
        // XXX maybe we should have a boolean variable, isClient??
        if(!(myRoles.get(0) instanceof transactions.Client)) {
            // if we are not a client, become a monitor and a transaction server
            
            log("becoming a monitor");
            monitorRole = new paxos.Monitor(this, services);
            myRoles.add(monitorRole);
            
            log("becoming a txserver");
            serverRole = new transactions.Server(this, services);
            myRoles.add(serverRole);
        }
    }

    @Override
    public Role[] getRoles() {
        return myRoles.toArray(new Role[] {});
    }
    
    // dump all of our Role's state to the log
    private void dumpInfo() {
        for(Role role : getRoles())
            role.dumpInfo();
    }

    //===================================================================================//
    //              RPC-related functionality (tx server and tx client)                  //
    //===================================================================================//   
    @Override
    public Handle<CommitResponse> handleCommitAttempt(int from, CommitAttempt txAttempt) {
        if(serverRole == null)
            throw new IllegalStateException("non-server received commit attempt");
        return serverRole.handleCommitAttempt(from, txAttempt); // dispatched to server
    }

    @Override
    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        if(serverRole == null)
            throw new IllegalStateException("non-client receieved data request");
        return serverRole.handleDataRequest(from, filename); // dispatched to server
    }

    @Override
    public void handleInvalidateRequest(int from, List<String> files) {
        if(clientRole == null)
            throw new IllegalStateException("non-server receieved invalidate request");
        clientRole.handleInvalidateRequest(from, files); // dispatched to client
    }
    
    @Override
    public List<ChosenTransaction> handleSyncRequest() {
        if(serverRole == null)
            throw new IllegalStateException("non-server receieved invalidate request");
        return serverRole.handleSyncRequest(); // dispatched to clien
    }
}
