package main;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Packet;

/**
 * This conveys the header for reliable, in-order message transfer. This is
 * carried in the payload of a Packet, and in turn the data being transferred is
 * carried in the payload of the RIOPacket packet.
 */
public class RIOPacket {

    public static final int MAX_PACKET_SIZE = Packet.MAX_PAYLOAD_SIZE;
    public static final int HEADER_SIZE = 14;
    public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

    public static final int FLAG_ACK = (1 << 0);
    public static final int FLAG_SYN = (1 << 1);
    public static final int FLAG_RST = (1 << 2);
    public static final int FLAG_PSH = (1 << 3); // "Push"  (KEEP-ALIVE)

    // Header fields:
    // (when you add or remove a field here, remember to change the HEADER_SIZE!!)
    private int protocol;    // 1 byte
    private int flags;        // 1 byte
    private int sourceID;    // 4 bytes
    private int destID;        // 4 bytes
    private int seqNum;        // 4 bytes
    
    private byte[] payload;

    /**
     * Constructing a new RIO packet.
     * @param protocol The application protocol attached to this packet.
     * @param flags Some bitwise OR'd combination of the FLAG_* constants.
     * @param seqNum The sequence number of the packet
     * @param payload The payload of the packet.
     */
    public RIOPacket(int protocol, int flags, int sourceID, int destID,
            int seqNum, byte[] payload) throws IllegalArgumentException {
        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("Illegal arguments given to RIOPacket");
        }

        this.protocol = protocol;
        this.flags = flags;
        this.sourceID = sourceID;
        this.destID = destID;
        this.seqNum = seqNum;
        this.payload = payload;
    }
    
    /**
     * @return whether this packet is an acknowledgment packet.
     */
    public boolean isACK() {
        return (flags & FLAG_ACK) != 0;
    }
    
    /**
     * @return whether this packet is a synchronization packet. 
     */
    public boolean isSYN() {
        return (flags & FLAG_SYN) != 0;
    }
    
    /**
     * @return whether this packet is a connection reset packet.
     */
    public boolean isRST() {
        return (flags & FLAG_RST) != 0;
    }
    
    /**
     * @return whether this packet is a push (keep-alive) packet
     */
    public boolean isPSH() {
        return (flags & FLAG_PSH) != 0;
    }

    /**
     * @return The protocol number
     */
    public int getProtocol() {
        return this.protocol;
    }
    
    /**
     * @return The sequence number
     */
    public int getSeqNum() {
        return this.seqNum;
    }

    /**
     * @return The payload
     */
    public byte[] getPayload() {
        return this.payload;
    }

    /**
     * 
     * @return The source ID
     */
    public int getSourceID() {
        return sourceID;
    }

    /**
     * 
     * @return the dest ID
     */
    public int getDestID() {
        return destID;
    }
    
    // XXX: should destID be immutable?
    void setDestID(int destID) {
        this.destID = destID;
    }

    /**
     * Convert the RIOPacket packet object into a byte array for sending over the wire.
     * @return A byte[] for transporting over the wire. Null if failed to pack for some reason.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);

            out.writeByte(protocol);
            out.writeByte(flags);
            out.writeInt(sourceID);
            out.writeInt(destID);
            out.writeInt(seqNum);

            out.write(payload, 0, payload.length);

            out.flush();
            out.close();
            return byteStream.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Unpacks a byte array to create a RIOPacket object.
     * Assumes the array has been formatted using pack method in RIOPacket.
     * @param packet String representation of the transport packet
     * @return RIOPacket Object created or null if the byte[] representation was corrupted
     */
    public static RIOPacket unpack(byte[] packet) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

            int protocol = in.readByte();
            int flags = in.readByte();
            int sourceID = in.readInt();
            int destID = in.readInt();
            int seqNum = in.readInt();

            byte[] payload = new byte[packet.length - HEADER_SIZE];
            if(payload.length > 0) {
                int bytesRead = in.read(payload, 0, payload.length);
                if (bytesRead != payload.length) {
                    return null;
                }
            }

            return new RIOPacket(protocol, flags, sourceID, destID, seqNum, payload);
        } catch (IllegalArgumentException e) {
            // will return null
        } catch(IOException e) {
            // will return null
        }
        return null;
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("RIOPacket");
        if(isACK()) sb.append(" ACK");
        if(isSYN()) sb.append(" SYN");
        if(isRST()) sb.append(" RST");
        sb.append(", protocol=" + protocol);
        sb.append(", srcID=" + sourceID);
        sb.append(", dstID=" + destID);
        sb.append(", seq=" + seqNum);
        return sb.toString();
    }
}
