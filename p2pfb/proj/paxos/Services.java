package paxos;

import java.util.Set;
import java.util.SortedSet;

import main.FSUtil;

/**
 * A Service provides node-specific services to Roles. For example, all roles
 * need to log events -- they use their reference to a Services object to do so.
 */
public interface Services {
    /**
     * @return the total number of nodes in the system
     */
    int getNodeCount();
    
    /**
     * @return the addresses of all nodes acting as acceptors. Agnostic to whether the node is up or down
     */
    Set<Integer> getAcceptorAddrs();
    
    /**
     * @return the addresses of all nodes acting as learners. Agnostic to whether the node is up or down
     */
    Set<Integer> getLearnerAddrs();
    
    /**
     * @return the address of all nodes participating in Paxos -- excludes clients
     */
    SortedSet<Integer> getPaxosAddrs();
    
    /**
     * Print a colored log message associated with the given role and node to STDOUT
     * @param role A reference to this
     * @param message the message to log
     */
    void logPaxos(Role role, String message);
    
    /**
     * @return An instance of FSUtil suitable for accessing the underlying filesystem.
     */
    FSUtil getFileSystem();
    
    /**
     * Return the instance of T that is running on this node, or null if no such role is running
     * 
     * @param <T> The {@link Role} you are interested in 
     * @return A reference to the instance
     */
    <T extends Role> T getRole(Class<T> klass);
}
