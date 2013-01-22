package tests;
import java.util.ArrayList;
import java.util.List;

import main.AppUtil;
import main.ErrorCode;
import main.RPCNode;
import primitives.Handle;
import proto.RPCProtos.ChosenTransaction;
import proto.RPCProtos.CommitAttempt;
import proto.RPCProtos.CommitResponse;
import proto.RPCProtos.DataResponse;
import proto.RPCProtos.Update;

// Allows us to test the RPC layer in isolation
public class RPCTester extends RPCNode {
	public static double getFailureRate() { return 0/100.0; }
    public static double getDropRate() { return 0/100.0; }
    public static double getDelayRate() { return 0/100.0; }
    
    /**
     * ctor
     */
    public RPCTester() {
        super();
    }
    
    @Override
    public void onCommand(String command) {
        try {
            String[] args = null;
            boolean unrecognized = false;
            if (command.startsWith("dataRequest ")) { // dataRequest dest filename
                args = AppUtil.splitArgs(command, 3);
                sendDataRequest(Integer.parseInt(args[1]), args[2])
                	.addListener(new CallResultListener<DataResponse>("dataRequest", this));
                
            } else if (command.startsWith("commitAttempt ")) { // commitAttempt dest
                args = AppUtil.splitArgs(command, 2);
                sendCommitAttempt(Integer.parseInt(args[1]), 0, new ArrayList<Update>())
                	.addListener(new CallResultListener<CommitResponse>("commitAttempt", this));
                
            } else if (command.startsWith("invalidateRequest ")) { // invalidateRequest dest
                args = AppUtil.splitArgs(command, 2);
                sendInvalidateRequest(Integer.parseInt(args[1]), new ArrayList<String>());
                
            } else {
                logError("unrecognized command: " + command.split(" ")[0]);
                unrecognized = true;
            }
            
            if(!unrecognized) {
                log("client called " + args[0]);
            }
        } catch(IllegalArgumentException e) {
            // This also catches NumberFormatException from Integer.parseInt()
            logError("bad command syntax");
        }
    }
    
    @Override
    public Handle<DataResponse> handleDataRequest(int from, String filename) {
        log("received data request from " + from);
        Handle<DataResponse> result = new Handle<DataResponse>();
        if(filename.equals("error")) {
            result.completedError(ErrorCode.ERR_IO_EXCEPTION);
        } else {
            DataResponse dr = DataResponse.newBuilder()
                .setContents("data query result")
                .setFilename(filename)
                .setVersion(0)
                .build();
            result.completedSuccess(dr);
        }
        return result;
    }
    
    @Override
    public void handleInvalidateRequest(int from, List<String> files) {
        log("received invalidate request from " + from);
    }
    
    @Override
    public Handle<CommitResponse> handleCommitAttempt(int from,
            CommitAttempt txAttempt) {
        log("received commit attempt from " + from);
        Handle<CommitResponse> result = new Handle<CommitResponse>();
        result.completedSuccess(null);
        return result;
    }
    
    @Override
    public List<ChosenTransaction> handleSyncRequest() {
        return null;
    }
}
