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

import org.apache.cassandra.config.KSMetaData;
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
 * different host from the ring. After a successful connecting, the ring will be
 * refreshed.
 *
 * This incorporates the CircuitBreaker pattern so not to overwhelm the network
 * with reconnect attempts.
 *
 */
public class CassandraProxyClient implements java.lang.reflect.InvocationHandler
{

    private static final Logger logger  = Logger.getLogger(CassandraProxyClient.class);

    /**
     * The initial host to create the proxy client.
     */
    private String              host;
    private int                 port;
    private final boolean       framed;
    /**
     * If true, randomly choose a server to connect after failure.
     * Otherwise, use round-robin mechanism.
     */
    private final boolean       randomizeConnections;
    /**
     * The last successfully connected server.
     */
    private String				lastUsedHost;
    /**
     * Last time the ring was checked.
     */
    private long                lastPoolCheck;
    /**
     * A list holds all servers from the ring.
     */
    private List<TokenRange>    ring;

    /**
     * Cassandra thrift client.
     */
    private Cassandra.Client    client;

    /**
     * The key space to get the ring information from.
     */
    private String              ringKs;
    private CircuitBreaker      breaker = new CircuitBreaker(1, 1);

    /**
     * Random generator for randomized connection.
     */
    private Random random;

    /**
     * Last server tried in round-robin mechanism.
     */
    private int lastUsedConnIndex;

    /**
     * Maximum number of attempts when connection is lost.
     */
    private final int maxAttempts = 10;

    /**
     * Construct a proxy connection.
     *
     * @param host cassandra host
     * @param port cassandra port
     * @param framed framed connection
     * @param randomizeConnections true if randomly choosing a server when connection fails; false to use round-robin mechanism
     * @return a Brisk Client Interface
     * @throws IOException
     */
    public static Brisk.Iface newProxyConnection(String host, int port, boolean framed, boolean randomizeConnections)
            throws IOException
    {
        return (Brisk.Iface) java.lang.reflect.Proxy.newProxyInstance(Brisk.Client.class.getClassLoader(),
                Brisk.Client.class.getInterfaces(), new CassandraProxyClient(host, port, framed,
                        randomizeConnections));
    }

    /**
     * Create connection to a given host.
     *
     * @param host cassandra host
     * @return cassandra thrift client
     * @throws IOException error
     */
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
        this.lastUsedHost = host;
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

    /**
     * Initialize the cassandra connection.
     *
     * @throws IOException
     */
    private void initialize() throws IOException
    {
        int attempt = 0;
        while (attempt++ < maxAttempts)
        {
            attemptReconnect();
            if (client != null) {
            	break;
            } else {
            	// sleep and try again
	            try
	            {
	                Thread.sleep(1050);
	            } catch (InterruptedException e) {
	                throw new IOException(e);
	            }
            }
        }

        if(client == null)
            throw new IOException("Error connecting to node " + lastUsedHost);

    	//Find the first keyspace that's not system and assign it to the lastly used keyspace.
        try
        {
            List<KsDef> allKs = client.describe_keyspaces();

            if (allKs.isEmpty() || (allKs.size() == 1 && allKs.get(0).name.equalsIgnoreCase("system"))) {
                allKs.add(createTmpKs());
            }

            for(KsDef ks : allKs)
            {
                if(!ks.name.equalsIgnoreCase("system")) {
                    ringKs = ks.name;
                    break;
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        checkRing();
    }

    /**
     * Create a temporary keyspace. This will only be called when there is no keyspace except system defined on (new cluster).
     * However we need a keyspace to call describe_ring to get all servers from the ring.
     *
     * @return the temporary keyspace
     * @throws InvalidRequestException error
     * @throws TException error
     * @throws InterruptedException error
     */
    private KsDef createTmpKs() throws InvalidRequestException, TException, InterruptedException, SchemaDisagreementException
    {
        KsDef tmpKs = new KsDef("proxy_client_ks", "org.apache.cassandra.locator.SimpleStrategy", Arrays
                .asList(new CfDef[] {}));
        tmpKs.setStrategy_options(KSMetaData.optsWithRF(1));

        client.system_add_keyspace(tmpKs);

        return tmpKs;
    }

    /**
     * Refresh the server in the ring.
     *
     * @throws IOException
     */
    private void checkRing() throws IOException
    {
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
                    //Reset the round robin to 0
                    if (!randomizeConnections)
                    	lastUsedConnIndex = 0;

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

    /**
     * Choose next server that is different from the last used host
     * to try to connect. There are three choices.
     *
     * 1. Random to choose next server.
     * 2. Round-robin mechanism.
     *
     * @param host the last server tried
     */
    private String getNextServer(String host) {
    	String endpoint = host;

        List<String> endpoints = ring.get(random.nextInt(ring.size())).endpoints;

   	 	// pick a different node from the ring
        while (!endpoint.equals(host))
        {
        	int i = lastUsedConnIndex;
        	if (randomizeConnections)
        		i = random.nextInt(endpoints.size());

        	endpoint = endpoints.get(i);
        	if (!randomizeConnections)
        	{
        		i ++;
        		//Start from beginning if reaches to the last server in the ring.
        		if (i == endpoints.size())
        			i = 0;
        	}
        }

        return endpoint;
    }

    /**
     * Attempt to connect to the next available server.
     */
    private void attemptReconnect()
    {
        // first try to connect to the same host as before
        if (!randomizeConnections || ring == null || ring.size() == 0)
        {
            try
            {
                client = createConnection(lastUsedHost);
                breaker.success();
                if(logger.isDebugEnabled())
                    logger.debug("Connected to cassandra at " + lastUsedHost + ":" + port);
                return;
            }
            catch (IOException e)
            {
                logger.warn("Connection failed to Cassandra node: " + lastUsedHost + ":" + port + " " + e.getMessage());
            }
        }

    	//If the ring does't contain any server, fails the attempt.
        if (ring == null || ring.size() == 0)
        {
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

        String endpoint = getNextServer(lastUsedHost);

        try
        {
            client = createConnection(endpoint);
            lastUsedHost = endpoint; //Assign the last successfully connected server.
            breaker.success();
            checkRing(); //Refresh the servers in the ring.
            logger.info("Connected to cassandra at " + endpoint + ":" + port);
        }
        catch (IOException e)
        {
            logger.warn("Failed connecting to a different cassandra node in this ring: " + endpoint + ":" + port);

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
            if (client == null)
            {
                // don't even try if client isn't connected
                breaker.failure();
            }

            try
            {
                if (breaker.allow())
                {
                    result = m.invoke(client, args);

                    if(m.getName().equalsIgnoreCase("set_keyspace") && args.length == 1)
                    {
                    	//Keep last known keyspace when set_keyspace is successfully invoked.
                        ringKs = (String)args[0];
                    }

                    breaker.success();
                    return result;
                }
                else
                {
                    while (!breaker.allow())
                    {
                        Thread.sleep(1050); // sleep and try again
                    }
                    attemptReconnect();

                    if(client != null)
                    {
                    	//If able to connect to a server, decrease tries to try more times.
                        tries--;
                    }
                }
            }
            catch (InvocationTargetException e)
            {

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