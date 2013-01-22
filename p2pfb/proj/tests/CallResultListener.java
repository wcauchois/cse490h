package tests;

import primitives.Handle;
import primitives.Logger;
import main.AppUtil;

/**
 * A simple Listener that prints the success/fail results to the screen
 *
 * @param <T> the type of the handle result
 */
public class CallResultListener<T> extends Handle.Listener<T> {
	private String callName;
    private  Logger logger;
    
    /**
     * ctor
     * 
     * @param callName the name of the RPC which this Listener is associated with
     * @param rpcNode a reference to our parent for logging purposes
     */
	public CallResultListener(String callName, Logger logger) {
		this.callName = callName;
		this.logger = logger;
	}
	@Override
	public void onError(int errorCode) {
	    logger.logError("call " + callName + " failed with code " + errorCode);
	}
	@Override
	public void onSuccess(T result) {
	    logger.log("call " + callName + " succeeded." + ((result != null) ? " the result was: " : ""));
	    if(result != null)
	    	AppUtil.printColored(result.toString(), AppUtil.COLOR_PURPLE);
	}
}
