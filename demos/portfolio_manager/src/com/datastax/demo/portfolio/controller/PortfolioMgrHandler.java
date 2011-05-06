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
package com.datastax.demo.portfolio.controller;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.datastax.demo.portfolio.Portfolio;
import com.datastax.demo.portfolio.Position;

import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class PortfolioMgrHandler implements com.datastax.demo.portfolio.PortfolioMgr.Iface
{
    private static final ColumnParent           pcp     = new ColumnParent("Portfolios");
    private static final SliceRange             range   = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                         ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1000);
    private static final SlicePredicate         sp      = new SlicePredicate().setSlice_range(range);
    private static final ColumnParent           scp     = new ColumnParent("Stocks");

    private static final ColumnParent           lcp     = new ColumnParent("HistLoss");
    private static final SlicePredicate         lcols   = new SlicePredicate();
    private static final ByteBuffer             lossCol = ByteBufferUtil.bytes("loss");
    private static final ByteBuffer             lossDateCol = ByteBufferUtil.bytes("worst_date");


    private static final ColumnParent           hcp      = new ColumnParent("StockHist");
    private static final SliceRange             hrange   = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
            ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 20);
    private static final SlicePredicate         hsp      = new SlicePredicate().setSlice_range(hrange);


    static
    {
        lcols.addToColumn_names(lossDateCol);
        lcols.addToColumn_names(lossCol);

    }

    private ThreadLocal<Cassandra.Iface> clients_ = new ThreadLocal<Cassandra.Iface>();

    public List<Portfolio> get_portfolios(String start_key, int limit) throws TException
    {
        KeyRange kr = new KeyRange(limit).setStart_key(start_key.getBytes()).setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        try
        {
            List<KeySlice> kslices = getClient().get_range_slices(pcp, sp, kr, ConsistencyLevel.ONE);            
            CqlResult result = getClient().execute_cql_query(ByteBufferUtil.bytes(buildPortfoliosQuery(start_key, limit)), Compression.NONE);

            //List<Portfolio> portfolios = buildPorfoliosFromRangeSlices(kslices);
            List<Portfolio> portfolios = buildPorfoliosFromCqlResult(result);
            
            addLossInformation(portfolios);
            addHistInformation(portfolios);

            return portfolios;
        }
        catch (Exception e)
        {
            throw new TException(e);
        }
    }

    private List<Portfolio> buildPorfoliosFromRangeSlices(List<KeySlice> kslices) throws Exception
    {
        List<Portfolio> portfolios = new ArrayList<Portfolio>();
        for (KeySlice ks : kslices)
        {
            Portfolio p = new Portfolio();
            p.setName(new String(ks.getKey()));

            Map<ByteBuffer, Long> tickerLookup = new HashMap<ByteBuffer, Long>();
            List<ByteBuffer> tickers = new ArrayList<ByteBuffer>();

            for (ColumnOrSuperColumn cosc : ks.getColumns())
            {
                tickers.add(cosc.getColumn().name);
                tickerLookup.put(cosc.getColumn().name, ByteBufferUtil.toLong(cosc.getColumn().value));
            }

            Map<ByteBuffer, List<ColumnOrSuperColumn>> prices = getClient().multiget_slice(tickers, scp, sp,
                    ConsistencyLevel.ONE);

            double total = 0;
            double basis = 0;

            Random r = new Random(Long.valueOf(new String(ks.getKey())));

            for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : prices.entrySet())
            {
                if (!entry.getValue().isEmpty())
                {
                    Double price = Double.valueOf(ByteBufferUtil.string(entry.getValue().get(0).column.value));
                    Position s = new Position(ByteBufferUtil.string(entry.getKey()), price, tickerLookup.get(entry.getKey()));
                    p.addToConstituents(s);
                    total += price * tickerLookup.get(entry.getKey());
                    basis += r.nextDouble() * 100 * tickerLookup.get(entry.getKey());

                }
            }


            p.setPrice(total);
            p.setBasis(basis);

            portfolios.add(p);

        }
        return portfolios;
    }
    
    private List<Portfolio> buildPorfoliosFromCqlResult(CqlResult result) throws Exception
    {
        List<Portfolio> portfolios = new ArrayList<Portfolio>();
        for (CqlRow row : result.rows)
        {
            Portfolio p = new Portfolio();
            p.setName(new String(row.getKey()));

            Map<ByteBuffer, Long> tickerLookup = new HashMap<ByteBuffer, Long>();
            List<ByteBuffer> tickers = new ArrayList<ByteBuffer>();

            for (Column cosc : row.getColumns())
            {
                tickers.add(cosc.name);
                tickerLookup.put(cosc.name, ByteBufferUtil.toLong(cosc.value));
            }
            
            double total = 0;
            double basis = 0;

            Random r = new Random(Long.valueOf(new String(row.getKey())));
            
            for ( ByteBuffer ticker : tickers )
            {
                CqlResult tResult = getClient().execute_cql_query(ByteBufferUtil.bytes(buildStocksQuery(ticker)), Compression.NONE);
                CqlRow tRow = tResult.getRowsIterator().hasNext() ? tResult.getRowsIterator().next() : null;
                if ( tRow != null ) {
                    Double price = Double.valueOf(ByteBufferUtil.string(tRow.columns.get(0).value));
                    Position s = new Position(ByteBufferUtil.string(tRow.key), price, tickerLookup.get(tRow.key));
                    p.addToConstituents(s);
                    total += price * tickerLookup.get(tRow.key);
                    basis += r.nextDouble() * 100 * tickerLookup.get(tRow.key);
                }
            }

            p.setPrice(total);
            p.setBasis(basis);

            portfolios.add(p);
        }
        return portfolios;
    }
    
    private static String buildPortfoliosQuery(String startKey, int limit)
    {
        return String.format("select FIRST 1000 ''..'' FROM Portfolios WHERE KEY >= '%s' LIMIT %d", startKey, limit);
    }
    
    private static String buildStocksQuery(ByteBuffer ticker) throws Exception
    {
     
        return String.format("select FIRST 1000 ''..'' FROM Stocks WHERE key = '%s'",ByteBufferUtil.bytesToHex(ticker));        
    }

    private Cassandra.Iface getClient()
    {

        Cassandra.Iface client = clients_.get();

        if (client == null)
        {
            try
            {

                client = CassandraProxyClient.newProxyConnection("localhost", 9160, true, ConnectionStrategy.RANDOM);

                client.set_keyspace("PortfolioDemo");
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            clients_.set(client);
        }

        return client;
    }

    private void addLossInformation(List<Portfolio> portfolios)
    {

        Map<ByteBuffer, Portfolio> portfolioLookup = new HashMap<ByteBuffer, Portfolio>();
        List<ByteBuffer> portfolioNames = new ArrayList<ByteBuffer>();

        for(Portfolio p : portfolios)
        {
            ByteBuffer name = ByteBufferUtil.bytes(p.name);
            portfolioLookup.put(name, p);
            portfolioNames.add(name);
        }


        try
        {
           Map<ByteBuffer,List<ColumnOrSuperColumn>> result =  getClient().multiget_slice(portfolioNames, lcp, lcols, ConsistencyLevel.ONE);

           for(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : result.entrySet())
           {
               Portfolio portfolio = portfolioLookup.get(entry.getKey());

               if(portfolio == null)
                   continue;

               for(ColumnOrSuperColumn col : entry.getValue())
               {
                   if(col.getColumn().name.equals(lossCol))
                       portfolio.setLargest_10day_loss(Double.valueOf(ByteBufferUtil.string(col.getColumn().value)));

                   if(col.getColumn().name.equals(lossDateCol))
                       portfolio.setLargest_10day_loss_date(ByteBufferUtil.string(col.getColumn().value));
               }
           }

        }
        catch (InvalidRequestException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (TException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (CharacterCodingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }


    private void addHistInformation(List<Portfolio> portfolios)
    {


        for(Portfolio p : portfolios)
        {
            ByteBuffer name = ByteBufferUtil.bytes(p.name);
            List<ByteBuffer> tickers = new ArrayList<ByteBuffer>();


            for( Position position : p.constituents)
            {
                tickers.add(ByteBufferUtil.bytes(position.ticker));
            }

            try
            {
                Map<ByteBuffer,List<ColumnOrSuperColumn>> result =  getClient().multiget_slice(tickers, hcp, hsp, ConsistencyLevel.ONE);

                Map<String, Double> histPrices = new LinkedHashMap<String,Double>();

                for(Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : result.entrySet())
                {

                    for(ColumnOrSuperColumn col : entry.getValue())
                    {
                        Double price = histPrices.get(ByteBufferUtil.string(col.column.name));

                        if(price == null)
                            price = 0.0;


                        price =+ Double.valueOf(ByteBufferUtil.string(col.column.value));

                        histPrices.put(ByteBufferUtil.string(col.column.name), price);

                    }

                }

                p.setHist_prices(Arrays.asList(histPrices.values().toArray(new Double[]{})));


            }
            catch (InvalidRequestException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (UnavailableException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (TimedOutException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (TException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (CharacterCodingException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
