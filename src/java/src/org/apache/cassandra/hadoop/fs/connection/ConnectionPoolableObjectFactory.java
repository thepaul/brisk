package org.apache.cassandra.hadoop.fs.connection;

import org.apache.commons.pool.BasePoolableObjectFactory;

public class ConnectionPoolableObjectFactory extends BasePoolableObjectFactory {
	
	private ConnectionFactory connectionFactory;

	public ConnectionPoolableObjectFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

    // for makeObject we'll simply return a new buffer
	@Override
    public Object makeObject() throws Exception { 
        return connectionFactory.createConnection();
    } 
     
    // when an object is returned to the pool,  
    // we'll clear it out 
	@Override
    public void passivateObject(Object obj) { 
        // (patricioe) do nothing for now. There is no way to close Cassandra.IFace.
    } 
	
}
