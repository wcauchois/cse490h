package paxos;

/**
 * Implemented by nodes running one or more roles.
 */
public interface Actor {
    /**
     * @return the roles this node is currently running
     */
    // invariant: no node may run more than one of the same type of role
    Role[] getRoles();
}