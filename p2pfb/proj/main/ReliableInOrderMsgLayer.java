package main;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery. In the event of failures, either a timeout will occur, or the server will
 * explicitly notify us with a RST packet that they have failed.  In either case, 
 * we maintain at-most-once semantics; the upper layer is notified when a failure
 * has occurred with the function onPeerFailure(). 
 * 
 */
public class ReliableInOrderMsgLayer {
    public static int ACK_TIMEOUT = 3;
    public static int PSH_TIMEOUT = 6;
    public static int MAX_RETRIES = 3;
    
    private HashMap<Integer, InChannel> inConnections;
    private HashMap<Integer, OutChannel> outConnections;
    private RIONode n;
    private int instanceID;
    
    RIONode getNode() {
        return n;
    }

    /**
     * Constructor.
     * 
     * @param RIONode our parent
     */
    public ReliableInOrderMsgLayer(RIONode n) {
        inConnections = new HashMap<Integer, InChannel>();
        outConnections = new HashMap<Integer, OutChannel>();
        this.n = n;
        this.instanceID = -1;
    }
    
    /**
     * Each instance of the RIONode is given a unique ID. This function sets
     * that ID. The ID should really be passed in the constructor, 
     * but Node.manager needs to be non-null, so we set it in Node.start() instead
     * 
     * @param instanceID
     */
    public void setInstanceID(int instanceID) {
        this.instanceID = instanceID;
    }
    
    /**
     * Receive a data packet.
     * 
     * @param from
     *            The address from which the data packet came
     * @param pkt
     *            The Packet of data
     */
    private void receiveData(int from, RIOPacket riopkt) {
        InChannel in = inConnections.get(from);
        
        if(in == null) {
            // If in == null and the client is still sending data to us that means we must have crashed,
            // so send an RST packet to inform the client and force them to start a new handshake.
            if(riopkt.getDestID() == this.instanceID) {
                // If we crashed, the client shouldn't know our new ID yet.
                // It's possible that we just timed out on them though, and they sent us a really delayed packet..
                return;
//                throw new RuntimeException("Received correct instance ID " + riopkt.getDestID() +
//                        " even though we just crashed!");
            }
            sendRST(from, riopkt);
            return;
        }
        
        if(riopkt.getSourceID() == in.getSourceInstanceID()) {
            sendACK(from, riopkt);
            
            LinkedList<RIOPacket> toBeDelivered = in.gotDataPacket(riopkt);
            for(RIOPacket p: toBeDelivered) {
                // deliver in-order the next sequence of packets
                n.onRIOReceive(from, p.getProtocol(), p.getPayload());
            }
        } else {
            // else if the srcID is wrong, ignore it -- we can assume that crashed clients will
            // always reinitiate a handshake
            System.out.println("Ignoring received DATA pkt, wrong sourceID " + riopkt.getSourceID());
        }
    }
    
    private void receivePSH(int from, RIOPacket riopkt) {
        InChannel in = inConnections.get(from);
        
        if(in == null) {
            // If in == null and the client is still sending data to us that means we must have crashed,
            // so send an RST packet to inform the client and force them to start a new handshake.
            if(riopkt.getDestID() == this.instanceID) {
                // If we crashed, the client shouldn't know our new ID yet.
                return;
//                throw new RuntimeException("Received correct instance ID " + riopkt.getDestID() + " even though we just crashed!");
            }
            sendRST(from, riopkt);
            return;
        } 
        
        if(riopkt.getSourceID() == in.getSourceInstanceID())
            sendPSHACK(from, riopkt);
    }
    
    /**
     * Send an ACK for a (valid) data packet
     * @param to
     * @param forPkt
     * precondition: forPkt.getDestInstanceID() == this.instanceID
     *               && forPkt.getSourceInstanceID() == inConnections.get(to).getSourceInstanceID()
     */
    private void sendACK(int to, RIOPacket forPkt) {
        RIOPacket ackPkt = new RIOPacket(forPkt.getProtocol(),
                RIOPacket.FLAG_ACK, forPkt.getSourceID(), forPkt.getDestID(),
                forPkt.getSeqNum(), new byte[0]);
        n.send(to, Protocol.RIO, ackPkt.pack());
    }
    
    /**
     * Send an PSH-ACK for a keep-alive probe
     * @param to
     * @param forPkt
     * precondition: forPkt.getDestInstanceID() == this.instanceID
     *               && forPkt.getSourceInstanceID() == inConnections.get(to).getSourceInstanceID()
     */
    private void sendPSHACK(int to, RIOPacket forPkt) {
        RIOPacket pshAckPkt = new RIOPacket(forPkt.getProtocol(),
                RIOPacket.FLAG_ACK | RIOPacket.FLAG_PSH, forPkt.getSourceID(), forPkt.getDestID(),
                forPkt.getSeqNum(), new byte[0]);
        n.send(to, Protocol.RIO, pshAckPkt.pack());
    }
    
    /**
     * Send an RST packet to let the client know that we crashed
     * 
     * @param to
     * @param forPkt
     */
    private void sendRST(int to, RIOPacket forPkt) {
        RIOPacket rstPkt = new RIOPacket(forPkt.getProtocol(),
                RIOPacket.FLAG_RST, forPkt.getSourceID(), forPkt.getDestID(),
                forPkt.getSeqNum(), new byte[0]); 
        n.send(to, Protocol.RIO, rstPkt.pack());
    }
    
    /**
     * Receive an acknowledgment packet.
     * 
     * @param from
     *            The address from which the data packet came
     * @param pkt
     *            The Packet of data
     */
    private void receiveACK(int from, RIOPacket ackPkt) {
        OutChannel out = outConnections.get(from);
        if(out != null &&                                        // If out is NULL, that means we crashed, so ignore it
                ackPkt.getSourceID() == instanceID &&            // If the source ID doesn't match, ignore the delayed ACK
                ackPkt.getDestID() == out.getDestInstanceID()    // If the dest ID doesn't match, ignore the ACK for a previous server
            ) {
            out.gotACK(ackPkt.getSeqNum());
        }
    }
    
    
    /**
     * Receive an keep-alive acknowledgment packet.
     * 
     * @param from
     *            The address from which the data packet came
     * @param pkt
     *            The Packet of data
     */
    private void receivePSHACK(int from, RIOPacket ackPkt) {
        OutChannel out = outConnections.get(from);
        if(out != null &&                                        // If out is NULL, that means we crashed, so ignore it
                ackPkt.getSourceID() == instanceID &&            // If the source ID doesn't match, ignore the delayed ACK
                ackPkt.getDestID() == out.getDestInstanceID()    // If the dest ID doesn't match, ignore the ACK for a previous server
            ) {
            out.gotPSHACK();
        }
    }

    /**
     * Send a packet using this reliable, in-order messaging layer. Note that
     * this method does not include a reliable, in-order broadcast mechanism.
     * 
     * @param destAddr
     *            The address of the destination for this packet
     * @param appProtocol
     *            The application-level protocol for the payload
     * @param payload
     *            The payload to be sent
     */
    public void send(int destAddr, int appProtocol, byte[] payload) {
        OutChannel out = outConnections.get(destAddr);
        if(out == null) {
            // If out is NULL, perform a handshake
            out = new OutChannel(this, destAddr);
            outConnections.put(destAddr, out);
            RIOPacket synPkt = new RIOPacket(
                    -1,                        // NULL protocol for control packets
                    RIOPacket.FLAG_SYN,
                    instanceID,
                    -1,                        // We don't know the dest ID yet
                    -1,                        // Use a sequence # of -1 for SYN packets
                    new byte[0]);            // No payload
            out.sendHandshakePacket(n, synPkt);
        }
      
        if(!out.hasHandshakeCompleted()) {
            // Handshake hasn't completed, so buffer the packet until then
            out.bufferPacket(appProtocol, payload);
        } else {
            out.sendRIODataPacket(n, appProtocol, payload);
        }
    }

    /**
     * Callback for timeouts while waiting for an ACK.
     * 
     * This method is here and not in OutChannel because OutChannel is not a
     * public class.
     * 
     * @param destAddr
     *            The receiving node of the unACKed packet
     * @param seqNum
     *            The sequence number of the unACKed packet
     * @param destInstanceID
     *            The instance ID of the instantiation we were trying to send to; this is
     *            so that we can ignore CallBacks that were registered before a failure occurred
     */
    public void onTimeout(Integer destAddr, Integer seqNum, Integer destInstanceID, Integer time) {
        OutChannel out = outConnections.get(destAddr);
        if(out != null && out.getDestInstanceID() == destInstanceID) {
            out.onTimeout(n, seqNum, time);
        }
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        Set<Integer> conns = new java.util.TreeSet<Integer>();
        conns.addAll(inConnections.keySet());
        conns.addAll(outConnections.keySet());
        for(Integer i: conns) {
            if(inConnections.containsKey(i))
                sb.append(" in[" + i + "]: " + inConnections.get(i).toString());
            if(outConnections.containsKey(i))
                sb.append(" out[" + i + "]: " +  outConnections.get(i).toString());
            
            sb.append("\n");
        }
        
        return sb.toString();
    }

    /**
     * Receive  a SYN packet from a client
     * 
     * @param from
     * @param synPkt
     */
    private void receiveSYN(Integer from, RIOPacket synPkt) {
        InChannel in = inConnections.get(from);
        if(in == null) {
            // We haven't encountered this peer before, so create a new InChannel
            in = new InChannel(synPkt.getSourceID());
            inConnections.put(from, in);
        } else if(in.getSourceInstanceID() != synPkt.getSourceID()) {
            // The client crashed, and is now trying to reinitiate a handshake
            handlePeerFailure(from);
            // However, we still need to create a new InChannel
            in = new InChannel(synPkt.getSourceID());
            inConnections.put(from, in);
        } // Else, the client sent a redundant SYN packet, so just resend the SYN-ACK
        
        RIOPacket synAckPkt = new RIOPacket(
                -1,                                            // No protocol
                RIOPacket.FLAG_ACK | RIOPacket.FLAG_SYN,    // SYN-ACK packet
                synPkt.getSourceID(),                        // Use the source ID from the SYN
                this.instanceID,                            // ...and our instance ID
                -1,                                            // No sequence number
                new byte[0]);
        n.send(from, Protocol.RIO, synAckPkt.pack());
    }
    
    /**
     * Receive a SYN-ACK packet from a server
     * 
     * @param from
     * @param synAckPkt
     */
    private void receiveSYNACK(Integer from, RIOPacket synAckPkt) {
        // Update our OutChannel's dest ID
        if(synAckPkt.getSourceID() == instanceID) {
            OutChannel out = outConnections.get(from);
            if(out == null)
                return;
//                throw new RuntimeException("Out connection should've existed: " + instanceID);
            out.setDestInstanceID(synAckPkt.getDestID());
            out.gotACK(synAckPkt.getSeqNum()); // Should be -1
            out.flushPacketBuffer();
        }
    }

    /**
     * Receive a RST packet from a crashed server
     * 
     * @param from
     * @param reConnPkt
     */
    private void receiveRST(Integer from, RIOPacket rstPkt) {
        OutChannel out = outConnections.get(from);
        // Ensure that our connection corresponds with the RST packet's view of the world
        // before resetting the connection.
        if(out != null &&
                rstPkt.getDestID() == out.getDestInstanceID() &&
                rstPkt.getSourceID() == instanceID) {
            handlePeerFailure(from); // Notify the client
        } else {
            // Else, ignore the RST
        }
        
    }
    
    /**
     *
     * @param peer The address of the peer that failed
     */
    void handlePeerFailure(int peer) {
        Collection<RIOPacket> failedSends = new ArrayList<RIOPacket>();
        OutChannel out = outConnections.get(peer);
        if(out != null) {
            failedSends = out.getUnAckedPackets();
        }
        inConnections.remove(peer);
        outConnections.remove(peer);
        n.onPeerFailure(peer, failedSends);
    }
    
    /**
     * @return our instanceID
     */
    public int getInstanceID() {
        return instanceID;
    }

    /**
     * Receive a RIO packet
     * 
     * @param from where the packet came from
     * @param msg the packet itself
     */
    public void receive(Integer from, byte[] msg) {
        RIOPacket rioPacket = RIOPacket.unpack(msg);
        
        if(rioPacket.isRST()) {
            // RST
            receiveRST(from, rioPacket);
        } else if(rioPacket.isACK() && !rioPacket.isSYN() && !rioPacket.isPSH()) {
            // DATA ACK
            receiveACK(from, rioPacket);
        } else if(rioPacket.isACK() && !rioPacket.isSYN() && rioPacket.isPSH()) {
            // PSH ACK
            receivePSHACK(from, rioPacket);
        } else if(rioPacket.isACK() && rioPacket.isSYN()) {
            // SYN-ACK
            receiveSYNACK(from, rioPacket);
        } else if(!rioPacket.isACK() && rioPacket.isSYN()) {
            // SYN
            receiveSYN(from, rioPacket);
        } else if(!rioPacket.isACK() && rioPacket.isPSH()) {
            // PSH (keep-alive)
            receivePSH(from, rioPacket);
        } else {
            // DATA
            receiveData(from, rioPacket);
        }
    }
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
    private int lastSeqNumDelivered;
    private HashMap<Integer, RIOPacket> outOfOrderMsgs;
    private int sourceInstanceID; // The instance ID of the node at the other end of this out channel
    
    InChannel(int sourceInstanceID){
        this.sourceInstanceID = sourceInstanceID;
        lastSeqNumDelivered = -1;
        outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
    }
 
    /**
     * 
     * @return the sourceInstanceID for this InChannel
     */
    public int getSourceInstanceID() {
        return sourceInstanceID;
    }

    /**
     * Method called whenever we receive a data packet.
     * 
     * @param pkt
     *            The packet
     * @return A list of the packets that we can now deliver due to the receipt
     *         of this packet
     */
    public LinkedList<RIOPacket> gotDataPacket(RIOPacket pkt) {
        LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
        int seqNum = pkt.getSeqNum();
        
        if(seqNum == lastSeqNumDelivered + 1) {
            // We were waiting for this packet
            pktsToBeDelivered.add(pkt);
            ++lastSeqNumDelivered;
            deliverSequence(pktsToBeDelivered);
        }else if(seqNum > lastSeqNumDelivered + 1){
            // We received a subsequent packet and should store it
            outOfOrderMsgs.put(seqNum, pkt);
        }
        // Duplicate packets are ignored
        
        return pktsToBeDelivered;
    }

    /**
     * Helper method to grab all the packets we can now deliver.
     * 
     * @param pktsToBeDelivered
     *            List to append to
     */
    private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
        while(outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
            ++lastSeqNumDelivered;
            pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
        }
    }
    
    @Override
    public String toString() {
        return "last delivered: " + lastSeqNumDelivered + ", # out of order pkts waiting: " + outOfOrderMsgs.size();
    }
}

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
    // keep-alive sequence number is -1
    private static final int SYN_SEQ = -1;
    // keep-alive sequence number is -2
    private static final int PSH_SEQ = -2;
    /**
     * Before a handshake has completed, we need to buffer the payloads of all
     * pending packets before encapsulating and sending them when the handshake
     * has completed succesfully 
     */
    private static class BufferedPacket {
        public int appProtocol;
        public byte[] payload;
        
        /**
         * 
         * @param appProtocol What type of data is stored in the payload
         * @param payload 
         */
        public BufferedPacket(int appProtocol, byte[] payload) {
            this.appProtocol = appProtocol;
            this.payload = payload;
        }
    }
    
    /**
     * We keep track of the number of times we have attempted to send
     * and unACKed packet. When the number of retries exceeds MAX_RETRIES,
     * we give up and notify the upper layers that a timeout error has occurred
     */
    private static class UnAckedPacket {
        public RIOPacket riopkt;
        public int tries; // We give up after MAX_RETRIES
        
        public UnAckedPacket(RIOPacket packet) {
            this.riopkt = packet;
            tries = 0;
        }
    }

    // -1 is reserved for an unacked syn packet, -2 is reserved for unacked keep-alive packet
    private HashMap<Integer, UnAckedPacket> unAckedPackets;
    private int lastSeqNumSent;
    private ReliableInOrderMsgLayer parent;
    private int destAddr;
    private int destInstanceID;
    private Queue<BufferedPacket> toBeSent;
    
    /**
     * @return the (non-control) RIOPackets which have not been ACked yet
     */
    public Collection<RIOPacket> getUnAckedPackets() {
        List<RIOPacket> packets = new ArrayList<RIOPacket>(unAckedPackets.size());
        for(UnAckedPacket unAckedPacket : unAckedPackets.values()) {
            if(unAckedPacket.riopkt.getSeqNum() != SYN_SEQ && unAckedPacket.riopkt.getSeqNum() != PSH_SEQ)
                packets.add(unAckedPacket.riopkt);
        }
        for(BufferedPacket pkt : toBeSent) {
            packets.add(new RIOPacket(pkt.appProtocol, 0, 0, 0, 0, pkt.payload));
        }
        return packets;
    }
    
    /**
     * @return the instanceID of the destination for our OutChannel
     */
    public int getDestInstanceID() {
        return destInstanceID;
    }
    
    /**
     * @return Whether or not the handshake has succesfully completed yet
     */
    public boolean hasHandshakeCompleted() {
        return destInstanceID != -1;
    }

    /**
     * Set the instanceID of the destination for our OutChannel as the last
     * step of the handshake
     * 
     * @param destInstanceID
     */
    public void setDestInstanceID(int destInstanceID) {
        if(destInstanceID == -1) 
            throw new IllegalStateException("illegal destInstanceID");
        
        this.destInstanceID = destInstanceID;
        
        // enqueue our keep-alive stream
        RIOPacket pshPkt = new RIOPacket(Protocol.RIO, RIOPacket.FLAG_PSH, parent.getInstanceID(), 
                this.destInstanceID, PSH_SEQ, new byte[0]);
        unAckedPackets.put(PSH_SEQ, new UnAckedPacket(pshPkt));
        addTimeout(parent.getNode(), destAddr, PSH_SEQ, ReliableInOrderMsgLayer.PSH_TIMEOUT);
    }
    
    /**
     * When the handshake has not completed yet, buffer the packets
     * to be sent later
     * 
     * @param appProtocol
     * @param payload
     */
    public void bufferPacket(int appProtocol, byte[] payload) {
        toBeSent.add(new BufferedPacket(appProtocol, payload));
    }
    
    /**
     * When the handshake has completed, flush the pending packets
     * 
     * precondition: destID != -1
     */
    public void flushPacketBuffer() {
        for(BufferedPacket pkt : toBeSent) {
            sendRIODataPacket(parent.getNode(), pkt.appProtocol, pkt.payload);
        }
        toBeSent.clear();
    }
    
    OutChannel(ReliableInOrderMsgLayer parent, int destAddr) {
        this(parent, destAddr, -1);
    }
    
    OutChannel(ReliableInOrderMsgLayer parent, int destAddr, int destInstanceID) {
        this.lastSeqNumSent = -1;
        this.destInstanceID = destInstanceID; // We don't know the instance ID of the destination until
                             // we perform our handshake.
        this.unAckedPackets = new HashMap<Integer, UnAckedPacket>();
        this.toBeSent = new LinkedList<BufferedPacket>();
        this.parent = parent;
        this.destAddr = destAddr;
    }
    
    /**
     * Send a new RIOPacket out on this channel.
     * 
     * @param n
     *            The sender and parent of this channel
     * @param protocol
     *            The protocol identifier of this packet
     * @param payload
     *            The payload to be sent
     */
    protected void sendRIODataPacket(RIONode n, int appProtocol, byte[] payload) {
        try{
            RIOPacket newPkt = new RIOPacket(appProtocol, 0, parent.getInstanceID(), 
                    this.destInstanceID, ++lastSeqNumSent, payload);
            unAckedPackets.put(newPkt.getSeqNum(), new UnAckedPacket(newPkt));
                        
            n.send(destAddr, Protocol.RIO, newPkt.pack());
            addTimeout(n, destAddr, lastSeqNumSent, ReliableInOrderMsgLayer.ACK_TIMEOUT);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    private void addTimeout(RIONode n, Integer destAddr, Integer seqNum, int time) {
        Method onTimeoutMethod = null;
        try {
            onTimeoutMethod = Callback.getMethod("onTimeout", parent,
                    new String[]{ "java.lang.Integer", "java.lang.Integer", "java.lang.Integer", "java.lang.Integer"});
        } catch(Exception e) { }
        n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum, destInstanceID, time }), time);
    }
    
    /**
     * Send a SYN packet, and register it to be retried if an ACK is not received
     * 
     * @param n our parent
     * @param riopkt the SYN
     */
    protected void sendHandshakePacket(RIONode n, RIOPacket riopkt) {
        try{
            unAckedPackets.put(SYN_SEQ, new UnAckedPacket(riopkt));
            n.send(destAddr, Protocol.RIO, riopkt.pack());
            addTimeout(n, destAddr, SYN_SEQ, ReliableInOrderMsgLayer.ACK_TIMEOUT);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Called when a timeout for this channel triggers
     * 
     * @param n
     *            The sender and parent of this channel
     * @param seqNum
     *            The sequence number of the unACKed packet
     */
    public void onTimeout(RIONode n, Integer seqNum, int time) {
        if(unAckedPackets.containsKey(seqNum)) {
            resendRIOPacket(n, seqNum, time);
        }
    }
    
    /**
     * Called when we get an ACK back. Removes the outstanding packet if it is
     * still in unACKedDataPackets.
     * 
     * @param seqNum
     *            The sequence number that was just ACKed
     */
    protected void gotACK(int seqNum) {
        unAckedPackets.remove(seqNum);
    }
    
    /**
     * Called whenever we get a keep-alive response.
     */
    protected void gotPSHACK() {
        if(unAckedPackets.containsKey(PSH_SEQ))
            unAckedPackets.get(PSH_SEQ).tries = 0;
        // else, ignore it
    }
    
    /**
     * Resend an unACKed packet.
     * 
     * @param n
     *            The sender and parent of this channel
     * @param seqNum
     *            The sequence number of the unACKed packet
     */
    private void resendRIOPacket(RIONode n, int seqNum, int time) {
        try{
            RIOPacket riopkt = null;
            UnAckedPacket unAckedPacket = unAckedPackets.get(seqNum);
            if(++unAckedPacket.tries <= ReliableInOrderMsgLayer.MAX_RETRIES) {
                riopkt = unAckedPacket.riopkt;
                n.send(destAddr, Protocol.RIO, riopkt.pack());
                addTimeout(n, destAddr, seqNum, time);
            } else {
                parent.handlePeerFailure(destAddr);
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public String toString() {
        return "unAckedPackets " + unAckedPackets;
    }
}
