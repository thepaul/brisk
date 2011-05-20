package org.apache.cassandra.hadoop.fs;

import java.util.HashSet;
import java.util.concurrent.Future;

public class StatusHolder {
	
	/**
	 * Holds a Set of status per thread.
	 */
    private static ThreadLocal<HashSet<Future<Boolean>>> statuses = new ThreadLocal<HashSet<Future<Boolean>>>();
    
    public void addStatus(Future<Boolean> status) {
    	HashSet<Future<Boolean>> futureSet = statuses.get();
    	
    	if (futureSet == null) {
    		futureSet = new HashSet<Future<Boolean>>();
    		statuses.set(futureSet);
    	}
    	
    	futureSet.add(status);
    }
    
    /**
     * Retrieves the Futures 
     * @throws StatusHolderException 
     */
    public void waitForCompletion() throws StatusHolderException {
    	HashSet<Future<Boolean>> futureSet = statuses.get();
    	
    	if (futureSet == null) {
    		// Nothing to release
    		return;
    	}
    	
    	Boolean aState;
    	for (Future<Boolean> future : futureSet) {
    		try {
    			aState = future.get();
    		} catch (Exception e) {
    			statuses.remove();
    			throw new StatusHolderException("One of the threads did not finish successfuly", e);
    		}
			
			if (aState) {
				statuses.remove();
				throw new StatusHolderException("One of the threads did not finish successfuly");
			}
		}
    	
    	// If no Exception, then remove the entries.
    	statuses.remove();
    }
    
    
    public class StatusHolderException extends Exception {

		public StatusHolderException(String arg0, Exception e) {
			super(arg0);
		}
		
		public StatusHolderException(String arg0) {
			super(arg0);
		}

    }

}
