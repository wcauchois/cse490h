package main;
public class Protocol {
    // ============================================================================
    // Transmission Layer Protocols
    // ============================================================================
    public static final int RIO = 0;
    public static final int PAXOS = 13;
    
    
    // ============================================================================
    // Application Layer Protocols
    // ============================================================================
    public static final int SIMPLESEND_PKT = 9;
    public static final int RIOTEST_PKT = 10;
    public static final int RPC = 11;
    public static final int CACHETEST_PACKET = 12;

    public static final int MAX_PROTOCOL = 127;
}
