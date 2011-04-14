package org.apache.cassandra.hadoop.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.EmbeddedBriskErrorServer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CassandraProxyClientTest
{
    @Test
    public void testNodeDown()
    {
        Brisk.Iface client = null;

        try
        {
            client = CassandraProxyClient.newProxyConnection("localhost", DatabaseDescriptor.getRpcPort(), true, true);
            fail("This should error");
        }
        catch (IOException e)
        {

        }

    }
    
    @Test
    public void testReconnect() throws Exception
    {

        
        EmbeddedBriskErrorServer.startBrisk();

        Brisk.Iface client = CassandraProxyClient.newProxyConnection("localhost", DatabaseDescriptor.getRpcPort(), true, true);
        List<KsDef> ks = client.describe_keyspaces();

        assertTrue(ks.size() > 0);

        try
        {
            client.get(ByteBufferUtil.EMPTY_BYTE_BUFFER, new ColumnPath("test"), ConsistencyLevel.ALL);
        }catch(TimedOutException e)
        {
            
        }
        
        assertEquals(11, client.get_count(ByteBufferUtil.EMPTY_BYTE_BUFFER, new ColumnParent("test"), new SlicePredicate(), ConsistencyLevel.ALL));
        

    }

}
