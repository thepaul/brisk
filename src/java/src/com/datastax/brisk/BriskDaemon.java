package com.datastax.brisk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.trackers.TrackerInitializer;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;

public class BriskDaemon extends org.apache.cassandra.service.AbstractCassandraDaemon
{

    private static Logger logger = LoggerFactory.getLogger(BriskDaemon.class);
    private ThriftServer server;
    
    protected void setup() throws IOException
    {
        super.setup();
            
        //Start hadoop trackers...
        if(System.getProperty("hadoop-trackers", "false").equalsIgnoreCase("true"))
            TrackerInitializer.init();
    }

    protected void startServer()
    {
        if (server == null)
        {
            server = new ThriftServer(listenAddr, listenPort);
            server.start();
        }
    }

    protected void stopServer()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServer extends Thread
    {
        private TServer serverEngine;

        public ThriftServer(InetAddress listenAddr, int listenPort)
        {
            // now we start listening for clients
            final BriskServer briskServer = new BriskServer();
            Brisk.Processor processor = new Brisk.Processor(briskServer);

            // Transport
            TServerSocket tServerSocket = null;

            try
            {
                tServerSocket = new TCustomServerSocket(new InetSocketAddress(listenAddr, listenPort),
                        DatabaseDescriptor.getRpcKeepAlive(),
                        DatabaseDescriptor.getRpcSendBufferSize(),
                        DatabaseDescriptor.getRpcRecvBufferSize());
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s",
                            listenAddr, listenPort), e);
            }

            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            // Protocol factory
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true,
                    true,
                    DatabaseDescriptor.getThriftMaxMessageLength());

            // Transport factory
            TTransportFactory inTransportFactory, outTransportFactory;
            if (DatabaseDescriptor.isThriftFramed())
            {
                int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
                inTransportFactory  = new TFramedTransport.Factory(tFramedTransportSize);
                outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
                logger.info("Using TFastFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);
            }
            else
            {
                inTransportFactory = new TTransportFactory();
                outTransportFactory = new TTransportFactory();
            }

            // ThreadPool Server
            CustomTThreadPoolServer.Options options = new CustomTThreadPoolServer.Options();
            options.minWorkerThreads = DatabaseDescriptor.getRpcMinThreads();
            options.maxWorkerThreads = DatabaseDescriptor.getRpcMaxThreads();

            ExecutorService executorService = new CleaningThreadPool(briskServer.clientState,
                    options.minWorkerThreads,
                    options.maxWorkerThreads);
            serverEngine = new CustomTThreadPoolServer(new TProcessorFactory(processor),
                    tServerSocket,
                    inTransportFactory,
                    outTransportFactory,
                    tProtocolFactory,
                    tProtocolFactory,
                    options,
                    executorService);
        }

        public void run()
        {
            logger.info("Listening for thrift clients...");
            serverEngine.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            serverEngine.stop();
        }
    }
    
    public static void main(String[] args)
    {
        new BriskDaemon().activate();
    }
    
}
