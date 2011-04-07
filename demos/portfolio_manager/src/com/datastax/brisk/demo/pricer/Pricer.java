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
package com.datastax.brisk.demo.pricer;

import java.io.PrintStream;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.cli.Option;

import com.datastax.brisk.demo.pricer.operations.PortfolioInserter;
import com.datastax.brisk.demo.pricer.operations.PriceInserter;
import com.datastax.brisk.demo.pricer.util.Operation;

import org.apache.cassandra.thrift.Cassandra;

public final class Pricer
{
    public static enum Operations
    {
        INSERT_PRICES, UPDATE_PORTFOLIOS
    }

    public static Session session;
    public static Random randomizer = new Random();

    /**
     * Producer-Consumer model: 1 producer, N consumers
     */
    private static final BlockingQueue<Operation> operations = new SynchronousQueue<Operation>(true);

    public static void main(String[] arguments) throws Exception
    {
        long latency, oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;

        try
        {
            session = new Session(arguments);
        }
        catch (IllegalArgumentException e)
        {
            printHelpMessage();
            return;
        }

       
        session.createKeySpaces();
        

        int threadCount  = session.getThreads();
        Thread[] consumers = new Thread[threadCount];
        PrintStream out = session.getOutputStream();

        out.println("total,interval_op_rate,interval_key_rate,avg_latency,elapsed_time");

        int itemsPerThread = session.getKeysPerThread();
        int modulo = session.getNumKeys() % threadCount;

        // creating required type of the threads for the test
        for (int i = 0; i < threadCount; i++)
        {
            if (i == threadCount - 1)
                itemsPerThread += modulo; // last one is going to handle N + modulo items

            consumers[i] = new Consumer(itemsPerThread);
        }

        new Producer().start();

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
        {
            consumers[i].start();
        }

        // initialization of the values
        boolean terminate = false;
        latency = 0;
        epoch = total = keyCount = 0;

        int interval = session.getProgressInterval();
        int epochIntervals = session.getProgressInterval() * 10;
        long testStartTime = System.currentTimeMillis();

        while (!terminate)
        {
            Thread.sleep(100);

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                oldTotal    = total;
                oldLatency  = latency;
                oldKeyCount = keyCount;

                total    = session.operations.get();
                keyCount = session.keys.get();
                latency  = session.latency.get();

                int opDelta  = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;
                double latencyDelta = latency - oldLatency;

                long currentTimeInSeconds = (System.currentTimeMillis() - testStartTime) / 1000;
                String formattedDelta = (opDelta > 0) ? Double.toString(latencyDelta / (opDelta * 1000)) : "NaN";

                out.println(String.format("%d,%d,%d,%s,%d", total, opDelta / interval, keyDelta / interval, formattedDelta, currentTimeInSeconds));
            }
        }
    }

    private static Operation createOperation(int index)
    {
        switch (session.getOperation())
        {
          
            case INSERT_PRICES:
                return new PriceInserter(index);
                
            case UPDATE_PORTFOLIOS:
                return new PortfolioInserter(index);
        
        }

        throw new UnsupportedOperationException();
    }

    /**
     * Printing out help message
     */
    public static void printHelpMessage()
    {
        System.out.println("Usage: ./bin/stress [options]\n\nOptions:");

        for(Object o : Session.availableOptions.getOptions())
        {
            Option option = (Option) o;
            String upperCaseName = option.getLongOpt().toUpperCase();
            System.out.println(String.format("-%s%s, --%s%s%n\t\t%s%n", option.getOpt(), (option.hasArg()) ? " "+upperCaseName : "",
                                                            option.getLongOpt(), (option.hasArg()) ? "="+upperCaseName : "", option.getDescription()));
        }
    }

    /**
     * Produces exactly N items (awaits each to be consumed)
     */
    private static class Producer extends Thread
    {
        public void run()
        {
            for (int i = 0; i < session.getNumKeys(); i++)
            {
                try
                {
                    operations.put(createOperation(i));
                }
                catch (InterruptedException e)
                {
                    System.err.println("Producer error - " + e.getMessage());
                    return;
                }
            }
        }
    }

    /**
     * Each consumes exactly N items from queue
     */
    private static class Consumer extends Thread
    {
        private final int items;

        public Consumer(int toConsume)
        {
            items = toConsume;
        }

        public void run()
        {
            Cassandra.Client client = session.getClient();

            for (int i = 0; i < items; i++)
            {
                try
                {
                    operations.take().run(client); // running job
                }
                catch (Exception e)
                {
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }
            }
        }
    }

}
