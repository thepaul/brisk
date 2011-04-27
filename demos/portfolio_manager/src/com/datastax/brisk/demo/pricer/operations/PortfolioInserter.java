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

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.brisk.demo.pricer.Pricer;
import com.datastax.brisk.demo.pricer.util.Operation;

public class PortfolioInserter extends Operation
{

    public PortfolioInserter(int index)
    {
        super(index);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        String[] stocks  = generatePortfolio(session.getColumnsPerKey());
        List<Column> columns = new ArrayList<Column>();
        
        for (int i = 0; i < stocks.length; i++)
        {
            columns.add(new Column().setName(ByteBufferUtil.bytes(stocks[i])).setValue(ByteBufferUtil.bytes((long)Pricer.randomizer.nextInt(50))).setTimestamp(System.currentTimeMillis()));
        }
       
        String rawKey = String.valueOf(index);
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        record.put(ByteBufferUtil.bytes(rawKey), getColumnsMutationMap("Portfolios",columns));

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
                                rawKey,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }


    public static Map<String, List<Mutation>> getColumnsMutationMap(String cfName, List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put(cfName, mutations);

        return mutationMap;
    }
}
