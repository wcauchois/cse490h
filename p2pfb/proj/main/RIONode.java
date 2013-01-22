package main;
import java.io.IOException;
import java.util.Collection;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Extension to the Node class that adds support for a reliable, in-order
 * messaging layer.
 * 
 * Nodes that extend this class for the support should use RIOSend and
 * onRIOReceive to send/receive the packets from the RIO layer. The underlying
 * layer can also be used by sending using the regular send() method and
 * overriding the onReceive() method to include a call to super.onReceive()
 */
public abstract class RIONode extends Node {
    private ReliableInOrderMsgLayer RIOLayer;
    
    public static int NUM_NODES = 10;
    private static final String ID_FILENAME = "security_through_obscurity_id.txt";
    protected int instanceID;
    
    public RIONode() {
        RIOLayer = new ReliableInOrderMsgLayer(this);
    }
    
    @Override
    public void start() {
        try {
            instanceID = nextInstanceID();
            RIOLayer.setInstanceID(instanceID);
        } catch(IOException e) {
            e.printStackTrace();
            fail();
        }
    }
    
    /**
     * Assign a unique identifier to this node
     * 
     * @return The unique instance identifier for this node
     * @throws IOException 
     */
    private int nextInstanceID() throws IOException {
        int oldID;
        
        if (Utility.fileExists(this, ID_FILENAME)) {
            PersistentStorageReader idReader = null;
            try {
                idReader = getReader(ID_FILENAME);
                String oldIDString = idReader.readLine(); // we could fail here
                if(oldIDString == null) {
                    // then we crashed after we got the writer but before we could write the ID
                    oldID = -1;
                } else {
                    String[] oldIDs = oldIDString.split(":");
                    oldID = Integer.parseInt(oldIDs[oldIDs.length - 1]);
                }
            } finally {
                if(idReader != null)
                    idReader.close();
            }
        } else {
            oldID = -1;
        } 
        
        int newID = oldID + 1; // XXX 2^32 - 1 overflow?
        
        PersistentStorageWriter idWriter = null;
        try {
            idWriter = getWriter(ID_FILENAME, true);
            // we could also fail to write this line, in which case the node will restart and
            // read the most recent ID (OK -- since we crashed prematurely, this doesn't really
            // "count" as an incarnation).
            idWriter.write(":" + newID);
        } finally {
            if(idWriter != null) {
                idWriter.close();
            }
        }
        
        return newID;
    }

    /**
     * @param transProtocol
     *          The transmission protocol used
     */
    @Override
    public void onReceive(Integer from, int transProtocol, byte[] msg) {        
        if(transProtocol == Protocol.RIO) {
            RIOLayer.receive(from, msg);
        }
    }

    /**
     * Send a message using the reliable, in-order delivery layer
     * 
     * @param destAddr
     *            The address to send to
     * @param appProtocol
     *            The application-level protocol of the payload
     * @param payload
     *            The payload of the message
     */
    public void RIOSend(int destAddr, int appProtocol, byte[] payload) {
        RIOLayer.send(destAddr, appProtocol, payload);
    }

    /**
     * Method that is called by the RIO layer when a message is to be delivered.
     * 
     * @param from
     *            The address from which the message was received
     * @param appProtocol
     *            The application-level protocol for the message
     * @param msg
     *            The message that was received
     */
    public abstract void onRIOReceive(Integer from, int appProtocol, byte[] msg);
    
    /**
     * This method is called whenever the message layer detects that a peer has crashed.
     * In a blocking RPC system, we would throw an exception back to the client in this case.
     * Since this our event-driven model does not handle blocking, this methods "simulates"
     * an exception by letting the client know that an error occurred.
     * 
     * @param peer the address of the peer who failed
     * @param failedSends packets that failed to be sent
     */
    public abstract void onPeerFailure(int peer, Collection<RIOPacket> failedSends);
    
    public void log(String s) {
        AppUtil.printColored("Node " + addr + ": " + s, AppUtil.COLOR_BLUE);
    }
    
    public void logError(String s) {
        AppUtil.printColored("Node " + addr + ": " + s, AppUtil.COLOR_RED);
    }
    
    public void logAction(String s) {
        AppUtil.printColored("Node " + this.addr + ": " + s, AppUtil.COLOR_CYAN);
    }
    
    public void logSuccess(String s) {
        AppUtil.printColored("Node " + this.addr + ": " + s, AppUtil.COLOR_GREEN);
    }
    
    @Override
    public String toString() {
        return RIOLayer.toString();
    }
}
