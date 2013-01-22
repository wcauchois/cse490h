package paxos;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import paxos.Messages.Envelope;
import paxos.Messages.PrepareRequest;
import paxos.Messages.AcceptRequest;
import paxos.Messages.AcceptedProposal;
import paxos.Messages.Promise;
import paxos.Messages.Heartbeat;
import main.Protocol;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.washington.cs.cse490h.lib.Node;

/**
 * In Paxos there are three classes of agents:
 * <ul>
 * <li> {@link Acceptor} </li>
 * <li> {@link Proposer} </li>
 * <li> {@link Learner}  </li>
 * </ul>
 * 
 * Role is the abstract superclass of these agents.
 */
@SuppressWarnings("serial")
public abstract class Role {
    protected Services services;
    protected Node n;
    
    /**
     * ctor
     * 
     * @param node a reference to the node acting this role
     * @param services services provided to this role
     */
    public Role(Node node, Services services) {
        this.n = node;
        this.services = services;
    }
    
    // return the address of the node running this role
    protected int getAddr() {
        return n.addr;
    }
    
    // wrap the given message in a proto Envelope
    private Envelope.Builder buildEnvelope(AbstractMessage message) {
        // we infer the type of the message by iterating through the DispatchEntry table
        Messages.Type messageType = null;
        // TODO: we need a reverse map to make this lookup more efficient!
        for(Map.Entry<Messages.Type, DispatchEntry> pair : dispatchTable.entrySet()) {
            if(pair.getValue().argumentClass.equals(message.getClass())) {
                messageType = pair.getKey();
                break;
            }
        }
        if(messageType == null) {
            throw new RuntimeException("couldn't find message type for class " + message.getClass());
        }
        
        return Envelope.newBuilder()
            .setType(messageType)
            .setPayload(message.toByteString());
    }
    
    protected void send(int pxId, int destAddr, AbstractMessage message) {
        n.send(destAddr, Protocol.PAXOS, buildEnvelope(message).setPxId(pxId).build().toByteArray());
    }
    // send a message /without/ a paxos ID!
    protected void send(int destAddr, AbstractMessage message) {
        n.send(destAddr, Protocol.PAXOS, buildEnvelope(message).build().toByteArray());
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////
    
    // To mux/demux messages, we need to know:
    //  * the Role to send the message to
    //  * the type of the message (we pass the message directly to the implementation layer, so the
    //                             /message/ type is equivalent to the /argument/ type)
    //  * the method to call on the Role once we have unpacked the message f
    //  * the method to unpack the message from the envelope's payload
    @SuppressWarnings("unchecked")
    private static class DispatchEntry {
        // the role associate with this dispatch type
        public Class<? extends Role> roleClass;
        public Class<?> argumentClass;
        // the receiver method on that role that should be invoked when a message
        // is received. the signature should be like ResultType receiverMethod(int from, ArgumentType arg).
        // receiver methods can never take more or less than two parameters, as above
        public Method receiverMethod;
        // a method that parses the argument to receiverMethod from a ByteString
        public Method unpackArgumentMethod; // ByteString -> ArgumentType
        // ctor: takes the method to call on the Role once we have unpacked the message
        public DispatchEntry(Method recieverMethod) {
            this.receiverMethod = recieverMethod;
            // Reflection MAGIC! hocus pocus, open sesame
            this.roleClass = (Class<? extends Role>)receiverMethod.getDeclaringClass();
            try {
                Class<?>[] parameterTypes = receiverMethod.getParameterTypes();
                this.argumentClass = parameterTypes[parameterTypes.length - 1]; // the last argument
                // parseFrom is implemented by the protobuf code
                this.unpackArgumentMethod = argumentClass.getMethod("parseFrom", ByteString.class);
            } catch(NoSuchMethodException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        // Given the payload of an envelope, unpack it in to its appropriate message type
        public Object unpackArgument(ByteString payload) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
            return unpackArgumentMethod.invoke(null, payload);
        }
    }
    
    // message type -> DispatchEntry
    // when we get a message, we look up the appropriate DispatchEntry object using this table.
    // a DispatchEntry contains information about how to route that message to the appropriate role
    // and invoke a /receiver method/ on that role.
    private static Map<Messages.Type, DispatchEntry> dispatchTable = new HashMap<Messages.Type, DispatchEntry>() {{
        try {
            // proposer -> acceptor
            put(Messages.Type.PREPARE_REQUEST, new DispatchEntry(
                    AggregateAcceptor.class.getMethod("receivePrepareRequest", int.class, int.class, PrepareRequest.class)
                    ));
            
            // proposer -> acceptor
            put(Messages.Type.ACCEPT_REQUEST, new DispatchEntry(
                    AggregateAcceptor.class.getMethod("receiveAcceptRequest", int.class, int.class, AcceptRequest.class)
                    ));
            
            // acceptor -> proposer
            put(Messages.Type.PROMISE, new DispatchEntry(
                    AggregateProposer.class.getMethod("receivePromise", int.class, int.class, Promise.class)
                    ));
            
            // acceptor -> learner
            put(Messages.Type.ACCEPTED_PROPOSAL, new DispatchEntry(
                    AggregateLearner.class.getMethod("receiveAcceptedProposal", int.class, int.class, AcceptedProposal.class)
                    ));
            
            put(Messages.Type.HEARTBEAT, new DispatchEntry(
                    Monitor.class.getMethod("receiveHeartbeat", int.class, Heartbeat.class)
                    ));
            
        } catch(NoSuchMethodException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }};
    
    /**
     * Demultiplex a message by unpacking from raw bytes and invoking the associated receive* method on 
     *     the role that handles messages of this type.
     * @param actor A node instance running one or more paxos roles.
     * @param message The raw bytes to demultiplex
     * @param from Who we received the message from -- send the result here.
     * @throws Exception 
     */
    public static void demux(Actor actor, byte[] message, int from){
        try {
            Envelope messageEnvelope = Envelope.parseFrom(message);
            demux(actor, messageEnvelope, from);
        } catch(InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
    
    private static Role getRoleFromActor(Actor actor, Class<? extends Role> roleClass) {
        Role result = null;
        for(Role role : actor.getRoles()) {
            if(roleClass.equals(role.getClass())) {
                result = role;
                break;
            }
        }
        return result;
    }
    
    // contract: if the envelope does not have a pxid, pxid will be -1
    private static void dispatch(Actor actor, DispatchEntry dEntry, int pxId, ByteString payload, int from) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        // step 1: find the role that handles this message type and is associated with the specified runner
        Role targetRole = getRoleFromActor(actor, dEntry.roleClass);
        if(targetRole == null)
            throw new RuntimeException("received wrong message for current role");
        
        // step 2: deserialize the argument from the envelope payload
        Object deserializedArgument = dEntry.unpackArgument(payload);
        // step 3: invoke the appropriate receive* method on the role
        Object result = null;
        // XXX: should we be branching on whether the message has a pxid, OR whether
        // the receiverMethod takes 3 arguments? maybe we could perform a check
        if(pxId >= 0) {
            result = dEntry.receiverMethod.invoke(targetRole, pxId, from, deserializedArgument);
        } else {
            result = dEntry.receiverMethod.invoke(targetRole, from, deserializedArgument);
        }
        // step 4 (optional): if there's a result, serialize it and send it back
        if(result != null && result instanceof AbstractMessage && from >= 0) {
            Envelope.Builder envelopeBuilder = targetRole.buildEnvelope((AbstractMessage)result);
            // if the incoming message has a paxos id, we should use the same pxid on the outgoing message!
            if(pxId >= 0)
                envelopeBuilder.setPxId(pxId);
            targetRole.n.send(from, Protocol.PAXOS, envelopeBuilder.build().toByteArray());
        }
    }
    
    /**
     * Demultiplex a message by unpacking from raw bytes and invoking the associated receive* method on 
     *     the role that handles messages of this type.
     * 
     * @param actor A node instance running one or more paxos roles.
     * @param message The proto Envelope to demultiplex
     * @param from Who we received the message from -- send the result here.
     * @throws Exception 
     */
    public static void demux(Actor actor, Envelope messageEnvelope, int from) {
        transactions.Server server = (transactions.Server)getRoleFromActor(actor, transactions.Server.class);
        // if we're running a server, and we receive a packet with from < current address: we must
        // relinquish leadership!
        if(server != null && from < server.n.addr)
            server.relinquishLeadership(); // idempotent
        
        DispatchEntry dEntry = dispatchTable.get(messageEnvelope.getType());
        int pxId = messageEnvelope.hasPxId() ? messageEnvelope.getPxId() : -1;
        try {
            dispatch(actor, dEntry, pxId, messageEnvelope.getPayload(), from);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            if(e.getCause() instanceof Error) { // s/b instanceof NodeCrashException, but that isn't visible
                throw (Error)e.getCause(); /// NodeCrashExceptions must be caught by the framework
            }
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Dump this Role's info to the log
     */
    public abstract void dumpInfo();
}
