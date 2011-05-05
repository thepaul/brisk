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
package com.datastax.brisk.demo.pricer.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.brisk.demo.pricer.Pricer;
import com.datastax.brisk.demo.pricer.util.Operation;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.joda.time.LocalDate;

public class HistoricalPriceInserter extends Operation
{
    private static final LocalDate today = LocalDate.fromDateFields(new Date(System.currentTimeMillis()));
       
    public HistoricalPriceInserter(int idx)
    {
        super(idx);
    }

    public void run(Client client) throws IOException
    {

        //Create a stock price per day
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>(tickers.length);

        LocalDate histDate = today.minusDays(index);
        ByteBuffer histDateBuf = ByteBufferUtil.bytes(histDate.toString("yyyy-MM-dd"));
        
        for(String stock : tickers)
        {
            record.put(ByteBufferUtil.bytes(stock), genDaysPrices(histDateBuf));
        }
        

        long start = System.currentTimeMillis();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                client.batch_mutate(record, session.getConsistencyLevel());
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                histDate,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.addAndGet(tickers.length);
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }

    private Map<String,List<Mutation>> genDaysPrices(ByteBuffer date)
    {
        Map<String, List<Mutation>> prices = new HashMap<String,List<Mutation>>();
             
        Mutation m = new Mutation();
        m.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(
                new Column()
                .setName(date)
                .setValue(ByteBufferUtil.bytes(String.valueOf((double)(Pricer.randomizer.nextDouble()*1000))))
                .setTimestamp(System.currentTimeMillis()) 
                ));
        
        prices.put("StockHist", Arrays.asList(m));
        
        return prices;       
    }
}
