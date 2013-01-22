package paxos;

import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;

import main.FSUtil;
import edu.washington.cs.cse490h.lib.Node;
import paxos.Messages.AcceptRequest;
import paxos.Messages.AcceptedProposal;
import paxos.Messages.PrepareRequest;
import paxos.Messages.Promise;
import paxos.Messages.Proposal;

/**
 * An Acceptor receives PrepareRequests and AcceptRequests from {@link Proposer}s,
 * and notifies {@link Learner}s of any Proposals that it has accepted.
 */
public class Acceptor extends PaxosRole {
    private static final String LATEST_ACCEPTED_PROPOSAL = "PAXOS_latest_proposal_n.txt";
    private static final String HIGHEST_PREPARE_N = "PAXOS_highest_prepare_n.txt";
    
    private String getLatestAcceptedProposalFilename() {
        return getPxId() + "_" + LATEST_ACCEPTED_PROPOSAL;
    }
    private String getHighestPrepareNFilename() {
        return getPxId() + "_" + HIGHEST_PREPARE_N;
    }
    
    // the highest numbered proposal that we have ever accepted. we may accept more
    // proposals, but whenever a proposal has been /chosen/, it is an invariant that
    // the value will never change
    private Proposal latestAcceptedProposal;
    // the number of the highested-numbered prepare request to which we have responded
    private int highestPrepareRequestN;
    
    /**
     * ctor
     * 
     * @param node a reference to the node acting this role
     * @param services services provided to this role
     */
    public Acceptor(int pxId, Node node, Services services) {
        super(pxId, node, services);
        this.latestAcceptedProposal = loadLatestAcceptedProposal();
        Integer lastPrepareRequestN = services.getFileSystem().getID(HIGHEST_PREPARE_N);
        highestPrepareRequestN = (lastPrepareRequestN == null) ? -1 : lastPrepareRequestN;
    }
    
    // return the latestAcceptedProposal (which we have persisted to disc), or null if we have not
    // yet accepted any proposals
    private Proposal loadLatestAcceptedProposal() {
        FSUtil fileSystem = services.getFileSystem();
        String filename = getLatestAcceptedProposalFilename();
        if (fileSystem.fileExists(filename)) {
            try {
                return Proposal.parseFrom(
                        services.getFileSystem().getBinaryContents(filename));
            }  catch (NumberFormatException e) {
                throw new IllegalStateException("The contents: of " + filename +
                        " were not in the form: n,v", e);
                
            } catch (InvalidProtocolBufferException e) {
                services.logPaxos(this, "Invalid binary protobuf serialization!!!!");
                return null;  // I /think/ this can only happen if we crashed for the very first time trying to update our
                // proposalN. After that, .bin.temp will take care of everything
            } catch (IOException e) {
                e.printStackTrace(); // FUCK ME IN THE GOAT ASS, JAVA
                System.exit(1);
            }
        } 
        
        return null; // we haven't accepted a proposal yet
    }
    
    /**
     * If we have not promised to not accept proposals of a lower number, then we accept
     * the prepare request and return a promise to the {@link Proposer}. Else, we notify
     * the {@link Proposer} that we will not accept.
     * 
     * @param from
     * @param request
     * @return a Promise to return to from
     */
    public Promise receivePrepareRequest(int from, PrepareRequest request) {
        if(request.getN() <= highestPrepareRequestN) {
            n.logSynopticEvent("ACCEPTOR:IGNORE-PREPARE["+request.getN()+"]");
            // we have already responded to a prepare request numbered greater than
            // n, so we can safely ignore this request (also ignore duplicate requests)
            services.logPaxos(this, "received prepare request from " + from + ", ignoring...");
            // TODO: respond with a message to the client saying "Hey, I'm not going to accept your proposal, so try another one"
            return null;
        } else {
            n.logSynopticEvent("ACCEPTOR:ACCEPT-PREPARE"+"["+request.getN()+"]");
            services.logPaxos(this, "received prepare request from " + from
                    + ", responding with promise #" + request.getN()); // not to accept any numbers higher than N
            highestPrepareRequestN = request.getN();
            services.getFileSystem().updateID(getHighestPrepareNFilename(), highestPrepareRequestN);
            Promise.Builder promiseBuilder = Promise.newBuilder();
            if(latestAcceptedProposal != null)
                promiseBuilder.setAcceptedPreviously(latestAcceptedProposal);
            promiseBuilder.setPrepareN(request.getN());
            return promiseBuilder.build();
        }
    }
    
    /**
     * Receive an accept request from the given proposer. If we have not promised to not accept proposals
     * of a lower number, then we can accept the proposal, else we are obligated to ignore the request. If
     * we do accept, notify all learners
     * 
     * @param from
     * @param request
     */
    public void receiveAcceptRequest(int from, AcceptRequest request) {
        Proposal proposal = request.getProposal();
        int n = proposal.getN();
        // an acceptor can accept a proposal numbered n iff it has not responded to a 
        // prepare request having a number greater than n
        if(n < highestPrepareRequestN) {
            this.n.logSynopticEvent("ACCEPTOR:IGNORE-ACCEPT["+n+"]");
            services.logPaxos(this, "received accept request from " + from + ", but #" + n + " is < highest #!");
            return;
        } else {
            this.n.logSynopticEvent("ACCEPTOR:ACCEPT-ACCEPT["+n+"]");
            services.logPaxos(this, "received accept request from " + from + ", accepting proposal #" + n);
            // accept the proposal
            updateLatestAcceptedProposal(proposal);
            // notify the learners
            // XXX: just notify the distinguished learner?
            for(Integer learner : services.getLearnerAddrs()) {
                send(learner,
                        AcceptedProposal.newBuilder().setProposal(proposal).build());
            }
        }
    }

    // serialize the latest accepted proposal to disc and update our state
    private void updateLatestAcceptedProposal(Proposal proposal) {
        try {
            services.getFileSystem()
                .writeBinaryContents(getLatestAcceptedProposalFilename(), proposal.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        this.latestAcceptedProposal = proposal;
    }
    
    @Override
    public void dumpInfo() {
        services.logPaxos(this, "info:");
        services.logPaxos(this, "    latestAcceptedProposal: " + latestAcceptedProposal);
        services.logPaxos(this, "    highestPrepareRequestN: " + highestPrepareRequestN);
    }
}
