package tests;

import java.util.Collection;

import main.Protocol;
import main.RIONode;
import main.RIOPacket;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * Our simple test node for testing failures. Just knows how to send
 * a single message to the specified peer
 */
public class SimpleSenderNode extends RIONode {
	public static double getFailureRate() { return 0/100.0; }
	public static double getDropRate() { return 0/100.0; }
	public static double getDelayRate() { return 0/100.0; }

	@Override
	public void onPeerFailure(int peer, Collection<RIOPacket> failedSends) {
		log("was notified of failure of peer #" + peer);
		for(RIOPacket pkt : failedSends) 
		    log("   was notified of failed send " + pkt);
	}
	
	@Override
	public void start() { 
		super.start();
		log("starting up (id=" + instanceID + ")");
	}

	@Override
	public void onRIOReceive(Integer from, int appProtocol, byte[] msg) {
		if(appProtocol != Protocol.SIMPLESEND_PKT) {
			log("received unknown protocol " + appProtocol);
			return;
		}
		
		String payload = Utility.byteArrayToString(msg);
		log("received \"" + payload + "\" from " + from);
	}

	@Override
	public void onCommand(String command) {
		String[] parts = command.split(" ");
		if(parts[0].equals("send")) {
			if(parts.length < 3) {
				log("too few arguments for send");
				return;
			}
			int destAddr = Integer.parseInt(parts[1]);
			String payload = parts[2];
			log("sending \"" + payload + "\" to " + destAddr);
			RIOSend(destAddr, Protocol.SIMPLESEND_PKT, Utility.stringToByteArray(payload));
		} else {
			log("unknown command \"" + command + "\"");
		}
	}
}
