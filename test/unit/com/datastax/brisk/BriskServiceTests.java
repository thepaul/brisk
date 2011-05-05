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
