package org.apache.cassandra.hadoop.fs;

import java.util.HashSet;
import java.util.concurrent.Future;

public class StatusHolder {
	
	/**
	 * Holds a Set of status per thread.
	 */
    private HashSet<Future<Boolean>> statuses = new HashSet<Future<Boolean>>();
    
    public void addStatus(Future<Boolean> status) {
    	statuses.add(status);
    }
    
    /**
     * Retrieves the Futures 
     * @throws StatusHolderException 
     */
    public void waitForCompletion() throws StatusHolderException {
    	
    	try {
    		
    		Boolean thereWasError;
    		
	    	for (Future<Boolean> future : statuses) {
	    		thereWasError = future.get();
	    			
	    		if (thereWasError) {
	    			throw new StatusHolderException("One of the threads did not finish successfuly");
	    		}
			}
	    		
    	} catch (Exception e) {
			throw new StatusHolderException("One of the threads did not finish successfuly", e);
		} finally {
			statuses.clear();
		}
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
