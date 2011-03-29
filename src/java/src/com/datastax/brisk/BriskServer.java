package com.datastax.brisk;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Brisk;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.Brisk.Iface;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;

public class BriskServer extends CassandraServer implements Brisk.Iface
{

    public List<List<String>> describe_keys(String keyspace, List<ByteBuffer> keys) throws TException
    {
        List<List<String>> keyEndpoints = new ArrayList<List<String>>(keys.size());
        
        for(ByteBuffer key : keys)
        {
            List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, key);           
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

            List<String> hosts = new ArrayList<String>(endpoints.size());
            
            for(InetAddress endpoint : endpoints)
            {
                hosts.add(endpoint.getHostName()+":"+DatabaseDescriptor.getRpcPort());
            }
            
            keyEndpoints.add(hosts);
        }
        
        return keyEndpoints;
    }

}
