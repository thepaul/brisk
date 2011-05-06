/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.EmbeddedBriskErrorServer;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

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
            client = CassandraProxyClient.newProxyConnection("localhost", DatabaseDescriptor.getRpcPort(), true, ConnectionStrategy.STICKY);
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

        Brisk.Iface client = CassandraProxyClient.newProxyConnection("localhost", DatabaseDescriptor.getRpcPort(), true, ConnectionStrategy.STICKY);
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
