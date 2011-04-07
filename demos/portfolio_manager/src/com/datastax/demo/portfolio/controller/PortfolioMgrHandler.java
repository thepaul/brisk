package com.datastax.demo.portfolio.controller;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.demo.portfolio.LeaderBoard;
import com.datastax.demo.portfolio.Portfolio;
import com.datastax.demo.portfolio.Stock;

import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

public class PortfolioMgrHandler implements com.datastax.demo.portfolio.PortfolioMgr.Iface
{
    private final ColumnParent           pcp     = new ColumnParent("Portfolios");
    private final SliceRange             range   = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                         ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1000);
    private final SlicePredicate         sp      = new SlicePredicate().setSlice_range(range);
    private final ColumnParent           scp     = new ColumnParent("Stocks");

    private ThreadLocal<Cassandra.Iface> clients_ = new ThreadLocal<Cassandra.Iface>();

    public List<Portfolio> get_portfolios(String start_key, int limit) throws TException
    {
        KeyRange kr = new KeyRange(limit).setStart_key(start_key.getBytes()).setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        try
        {
            List<KeySlice> kslices = getClient().get_range_slices(pcp, sp, kr, ConsistencyLevel.ONE);

            List<Portfolio> portfolios = new ArrayList<Portfolio>();
            for (KeySlice ks : kslices)
            {
                Portfolio p = new Portfolio();
                p.setName("Portfolio #" + new String(ks.getKey()));

                List<ByteBuffer> tickers = new ArrayList<ByteBuffer>();

                for (ColumnOrSuperColumn cosc : ks.getColumns())
                {
                    tickers.add(cosc.getColumn().name);
                }

                Map<ByteBuffer, List<ColumnOrSuperColumn>> prices = getClient().multiget_slice(tickers, scp, sp,
                        ConsistencyLevel.ONE);

                double total = 0;
                for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : prices.entrySet())
                {
                    if (!entry.getValue().isEmpty())
                    {
                        Double price = Double.valueOf(ByteBufferUtil.string(entry.getValue().get(0).column.value));
                        Stock s = new Stock(ByteBufferUtil.string(entry.getKey()), price);
                        p.addToConstituents(s);
                        total += price;
                    }
                }

                p.setPrice(total);

                portfolios.add(p);

            }

            return portfolios;
        }
        catch (Exception e)
        {
            throw new TException(e);
        }

    }

    private Cassandra.Iface getClient()
    {

        Cassandra.Iface client = clients_.get();

        if (client == null)
        {
            try
            {
                client = CassandraProxyClient.newProxyConnection("localhost", 9160, true, true);

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

    public LeaderBoard get_leaderboard() throws TException
    {
        // TODO Auto-generated method stub
        return null;
    }

}
