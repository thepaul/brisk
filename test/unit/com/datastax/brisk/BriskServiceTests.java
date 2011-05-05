package com.datastax.brisk;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.utils.FBUtilities;

public class BriskServiceTests
{
    @Test
    public void testJobTrackerAddress() throws Exception
    {
        EmbeddedServer.startBrisk();

        Brisk.Iface client = CassandraProxyClient.newProxyConnection("localhost", DatabaseDescriptor.getRpcPort(), true, ConnectionStrategy.STICKY);
       
        assertEquals(FBUtilities.getLocalAddress().getHostName()+":8012",client.get_jobtracker_address());
    }
}
