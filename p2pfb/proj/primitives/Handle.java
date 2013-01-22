package primitives;
import java.util.ArrayList;
import java.util.List;

/**
 * A handle is a primitive for supporting operations that may span across several events.
 * You can think of it as a "promise" -- whenever the operation completes, the Handle will
 * notify all registered listeners that the operation has completed.
 * 
 * For example, when a client sends an RPC to a server, it may take several rounds for the
 * server to send an RPC return value to the client. The client can register a listener with
 * the handle for processing the return value at a later time.
 * 
 * @param <T> the onSuccess return type
 */
public class Handle<T> {
	/**
	 * Register a Listener with the Handle
	 */
	public static abstract class Listener<R> {
	    /**
	     * Tell us what to do when the operation completes successfully
	     * 
	     * @param result the result of the operation
	     */
	    public void onSuccess(R result) { }
	    
	    /**
	     * Tell us what to do when the operation fails
	     */
	    public void onError(int errorCode) { }
	}

	private boolean completed = false;
	private T result = null;
	private int errorCode = 0;
	private boolean error = false;
    private List<Listener<T>> listeners;

    /**
     * ctor
     */
    public Handle() {
        this.listeners = new ArrayList<Listener<T>>();
    }
    
    /**
     * Add a new listener to this handle to be notified when the operation has been completed.
     * If the operation completed before the listener was added, the listener will be notified
     * immediately.
     * 
     * @param listener the listener to add
     */
    public void addListener(Listener<T> listener) {
    	if(completed) {
    		if(error)
    			listener.onError(errorCode);
    		else
    			listener.onSuccess(result);
    	} else {
    		listeners.add(listener);
    	}
    }

    /**
     * Notify all listeners that the operation has completed
     * 
     * @param result the result of the operation
     */
    public Handle<T> completedSuccess(T result) {
        for(Listener<T> listener : listeners)
            listener.onSuccess(result);
        
        completed = true;
        error = false;
        this.result = result;
        return this;
    }
    
    /**
     * Notify all listeners that the operation has failed
     * 
     * @param errorCode what went wrong
     */
    public Handle<T> completedError(int errorCode) {
        for(Listener<T> listener : listeners)
            listener.onError(errorCode);
        
        completed = true;
        error = true;
        this.errorCode = errorCode;
        return this;
    }
    
    /**
     * Whenever an error occurs, propagate the error to the given handle
     * 
     * @param otherHandle the other handle to propagate errors to
     */
    public void propagateErrorsTo(final Handle<T> otherHandle) {
    	addListener(new Listener<T>() {
			@Override
			public void onError(int errorCode) {
				otherHandle.completedError(errorCode);
			}
    	});
    }
    
    /**
     * Whenever an error occurs, propagate the error to the given handle, and
     * release the given lock.
     * 
     * @param otherHandle the other handle to propagate errors to
     * @param l the lock to release upon error
     */
    public void propagateErrorsTo(final Handle<T> otherHandle, final Lock l) {
    	addListener(new Listener<T>() {
			@Override
			public void onError(int errorCode) {
				otherHandle.completedError(errorCode);
				l.release();
			}
    	});
    }
}

