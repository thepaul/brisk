package com.datastax.brisk;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;


/**
 * Lets us use the Cassandra thift API within Cassandra...
 * 
 * We need to work around the fact CassandraServer tracks client state per-thread
 */
public class BriskInternalServer extends BriskServer
{
    private String keySpace;
       
    @Override
    public void set_keyspace(String keyspace) throws InvalidRequestException, TException
    {
        this.keySpace = keyspace;
    }

    @Override
    public ClientState state()
    {
        ClientState state = super.state();
             
        state.setKeyspace(keySpace);
        
        return state;
    }   
}
