package primitives;
import java.util.LinkedList;
import java.util.Queue;


/**
 *  A lock is a primitive for serializing operations.
 *  
 *  We know this isn't a true lock, but it's ok because we're single threaded; we just use a
 *  single boolean rather than atomic test/set. Locks keeps track of which callbacks
 *  are blocking on this lock and executes them upon release().
 *
 */
public class Lock {
    /**
     * Tell us what to run whenever the lock is acquired
     */
	public interface Callback {
		void run(Lock l);
	}
	
	private Queue<Callback> blocked;
	private boolean acquired;
	
	/**
	 * ctor
	 */
	public Lock() {
		blocked = new LinkedList<Callback>();
		acquired = false;
	}
	
	@Override
	public String toString() {
		if(acquired) {
			return "acquired(" + blocked.size() + ")";
		} else {
			return "unacquired";
		}
	}
	
	/**
	 * acquire the lock. May block until the lock is unoccupied
	 * 
	 * @param callback
	 */
	public void acquire(Callback callback) {
		if(acquired) {
			blocked.add(callback);
		} else {
			acquired = true;
			callback.run(this);
		}
	}
	
	/**
	 * release the lock
	 * 
     */
	public void release() {
		if(!acquired)
			throw new IllegalStateException("tried to release unacquired lock");
		
		if(blocked.isEmpty()) {
			acquired = false;
		} else {
			blocked.poll().run(this);
		}
	}
}