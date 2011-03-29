package com.datastax.brisk;

import java.io.IOException;

import org.apache.cassandra.hadoop.trackers.TrackerInitializer;

public class CassandraDaemon extends org.apache.cassandra.thrift.CassandraDaemon
{

    protected void setup() throws IOException
    {
        super.setup();
            
        //Start hadoop trackers...
        if(System.getProperty("hadoop-trackers", "false").equalsIgnoreCase("true"))
            TrackerInitializer.init();
    }

    
    public static void main(String[] args)
    {
        new CassandraDaemon().activate();
    }
    
}
