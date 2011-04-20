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
	/**
	 * When a cassandra server is not up, we expect that an IOException is thrown out from the method.
	 */
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

    /**
     * The purpose of this test is to make sure that:
     * When a TimedOutException is thrown out from invoke method, the CassandraProxyClient will try at least maxAttempts times before
     * throwing the exception to the client.
     *
     * A counter from the ThriftServer in BriskErrorDaemon tracks how many times the method has been called.
     *
     * @throws Exception
     */
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
            fail("Expect a TimedoutException");
        } catch(TimedOutException e) {
        	//This is expected.
        }

        assertEquals(11, client.get_count(ByteBufferUtil.EMPTY_BYTE_BUFFER, new ColumnParent("test"), new SlicePredicate(), ConsistencyLevel.ALL));
    }

}
