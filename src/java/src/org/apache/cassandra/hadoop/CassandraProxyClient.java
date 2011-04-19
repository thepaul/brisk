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
package org.apache.cassandra.hadoop;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.CircuitBreaker;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;

/**
 * This wraps the underlying Cassandra thrift client and attempts to handle
 * disconnect, unavailable, timeout errors gracefully.
 *
 * On disconnect, if it cannot reconnect to the same host then it will use a
 * different host from the ring, which it periodically checks for updates to.
 *
 * This incorporates the CircuitBreaker pattern so not to overwhelm the network
 * with reconnect attempts.
 *
 */
public class CassandraProxyClient implements java.lang.reflect.InvocationHandler
{

    private static final Logger logger  = Logger.getLogger(CassandraProxyClient.class);

    private String              host;
    private int                 port;
    private final boolean       framed;
    private final boolean       randomizeConnections;
    private long                lastPoolCheck;
    private List<TokenRange>    ring;
    private Cassandra.Client    client;
    private String              ringKs;
    private CircuitBreaker      breaker = new CircuitBreaker(1, 1);
    private Random random;
    private int lastUsedConnIndex;
    private final int maxAttempts = 10;

    public static Brisk.Iface newProxyConnection(String host, int port, boolean framed, boolean randomizeConnections)
            throws IOException
    {
        return (Brisk.Iface) java.lang.reflect.Proxy.newProxyInstance(Brisk.Client.class.getClassLoader(),
                Brisk.Client.class.getInterfaces(), new CassandraProxyClient(host, port, framed,
                        randomizeConnections));
    }

    private Cassandra.Client createConnection(String host) throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to server", e);
        }

        Cassandra.Client client = new Brisk.Client(new TBinaryProtocol(trans));

        //connect to last known keyspace
        if(ringKs != null)
        {
            try
            {
                client.set_keyspace(ringKs);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
        }

        return client;
    }

    private CassandraProxyClient(String host, int port, boolean framed, boolean randomizeConnections)
            throws IOException
    {

        this.host = host;
        this.port = port;
        this.framed = framed;
        this.randomizeConnections = randomizeConnections;
        //If randomized to choose a connection, initialize the random generator.
        if (randomizeConnections) {
        	random = new Random();
        } else {
        	//If not randomized to choose a connection, use round robin mechanism.
        	lastUsedConnIndex = 0;
        }

        lastPoolCheck = 0;

        initialize();
    }

    private void initialize() throws IOException
    {
        int attempt = 0;
        while (client == null && attempt++ < maxAttempts)
        {
            attemptReconnect();
            try
            {
                Thread.sleep(1050);
            } catch (InterruptedException e) {
                throw new IOException(e);
            } // sleep and try again
        }

        if(client == null)
            throw new IOException("Error connecting to node");

        try
        {
            List<KsDef> allKs = client.describe_keyspaces();

            if (allKs.isEmpty() || (allKs.size() == 1 && allKs.get(0).name.equalsIgnoreCase("system"))) {
                allKs.add(createTmpKs());
            }

            for(KsDef ks : allKs)
            {
            	//Find the first keyspace that's not system and assign it to the lastly used keyspace.
                if(!ks.name.equalsIgnoreCase("system")) {
                    ringKs = ks.name;
                    //break;
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        checkRing();
    }

    private KsDef createTmpKs() throws InvalidRequestException, TException, InterruptedException
    {
    	//TODO: Is this necessary? This is to create a proxy_client_ks keyspace with replication factor only 1
    	//This proxy_client_ks will be used as the ringKs, and it will be used in checkRing() method to return
    	//the servers in the ring. In the junit test, this keyspace is created, however, describe_keyspaces doesn't
    	//return it.
        KsDef tmpKs = new KsDef("proxy_client_ks", "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays
                .asList(new CfDef[] {}));

        client.system_add_keyspace(tmpKs);

        return tmpKs;
    }

    private synchronized void checkRing() throws IOException
    {
    	//TODO: This method is called only under two circumstances:
    	//1. During initialization.
    	//2. in invoke method, when ring has not been initailized.
    	//When a server is not reachable, it might make sense to refresh the ring to remove the servers that are offline
    	//and add new servers to the ring.
    	//Or this should be checked periodically to refresh the ring.
        if (client == null) {
            breaker.failure();
            return;
        }

        long now = System.currentTimeMillis();

        if ((now - lastPoolCheck) > 60 * 1000) {
            try {
                if (breaker.allow()) {
                    ring = client.describe_ring(ringKs);
                    lastPoolCheck = now;

                    breaker.success();
                }
            } catch (TException e) {
                breaker.failure();
                attemptReconnect();
            } catch (InvalidRequestException e) {
                throw new IOException(e);
            }
        }

    }

    private void attemptReconnect()
    {
        // first try to connect to the same host as before
        if (!randomizeConnections || ring == null || ring.size() == 0) {
            try {
                client = createConnection(host);
                breaker.success();
                if(logger.isDebugEnabled())
                    logger.debug("Connected to cassandra at " + host + ":" + port);
                return;
            } catch (IOException e) {
                logger.warn("Connection failed to Cassandra node: " + host + ":" + port + " " + e.getMessage());
            }
        }

    	//If the ring does't contain any server, fails the attempt.
        if (ring == null || ring.size() == 0) {
            logger.warn("No cassandra ring information found, no other nodes to connect to");
            client = null;
            return;
        }

        // only one node (myself)
        if (ring.size() == 1)
        {
            logger.warn("No other cassandra nodes in this ring to connect to");
            client = null;
            return;
        }

        String endpoint = host;

        // pick a different node from the ring
        List<String> endpoints = ring.get(random.nextInt(ring.size())).endpoints;
        while (!endpoint.equals(host)) {
        	int i = lastUsedConnIndex;
        	if (randomizeConnections) {
        		i = random.nextInt(endpoints.size());
        	} else {
        		i ++;
        		//Start from beginning.
        		if (i == endpoints.size()) {
        			i = 0;
        		}
        	}
        	endpoint = endpoints.get(i);
        }

        try {
            client = createConnection(endpoint);
            breaker.success();
            logger.info("Connected to cassandra at " + endpoint + ":" + port);
        } catch (IOException e) {
            logger.warn("Failed connecting to a different cassandra node in this ring: " + endpoint + ":" + port);
            /*TODO:
             * we have already tried to connect to this server the first time, we should not try this again.
            try
            {
                client = createConnection(host);
                breaker.success();
                if(logger.isDebugEnabled())
                    logger.debug("Connected to cassandra at " + host + ":" + port);
            } catch (IOException e2) {
                logger.warn("Connection failed to Cassandra node: " + host + ":" + port);
            }*/

            client = null;
        }
    }

    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable
    {
        Object result = null;

        int tries = 0;

        // incase this is the first time
        if (ring == null)
            checkRing();

        while (result == null && tries++ < maxAttempts)
        {
            // don't even try if client isn't connected
            if (client == null) {
                breaker.failure();
            }

            try {

                if (breaker.allow()) {
                    result = m.invoke(client, args);

                    //Keep last known keyspace when set_keyspace is successfully invoked.
                    if(m.getName().equalsIgnoreCase("set_keyspace") && args.length == 1) {
                        ringKs = (String)args[0];
                    }

                    breaker.success();
                    return result;
                } else {

                    while (!breaker.allow()) {
                        Thread.sleep(1050); // sleep and try again
                    }
                    attemptReconnect();

                    if(client == null) {
                    	//If fails to connect to a server, decrease tries to try more times.
                    	//TODO: If for some reasons, we are unable to connect to any of the server, it will possibly
                    	//be a infinite loop here. Is this necessary?
                        tries--;
                    }
                }
            } catch (InvocationTargetException e) {

                if (e.getTargetException() instanceof UnavailableException ||
                        e.getTargetException() instanceof TimedOutException ||
                        e.getTargetException() instanceof TTransportException)
                {

                    breaker.failure();

                    // rethrow on last try
                    if (tries >= maxAttempts)
                        throw e.getCause();
                }
                else
                {
                    throw e.getCause();
                }
            }
            catch (Exception e)
            {
                logger.error("Error invoking a method via proxy: ", e);
                throw new RuntimeException(e);
            }

        }

        throw new UnavailableException();
    }

}