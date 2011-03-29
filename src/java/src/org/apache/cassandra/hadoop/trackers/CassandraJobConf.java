package org.apache.cassandra.hadoop.trackers;

import java.net.InetAddress;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

public class CassandraJobConf extends org.apache.hadoop.mapred.JobConf
{
    private static final Logger logger = Logger.getLogger(CassandraJobConf.class);
    
    public String get(String name, String defaultValue)
    {  
        if (name.equals("mapred.job.tracker") || name.equals("mapreduce.jobtracker.address"))     
        {
            String address = getJobTrackerNode().getHostName()+":8012";
                        
            return address;
        }

        return super.get(name, defaultValue);
    }

    public String get(String name)
    {        
        if (name.equals("mapred.job.tracker") || name.equals("mapreduce.jobtracker.address"))
            return getJobTrackerNode().getHostName()+":8012";
        
        return super.get(name);
    }

    //Will pick a live seed to use as a job tracker in this local dc
    public static InetAddress getJobTrackerNode()
    {
        
        //Get this nodes local DC
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getLocalAddress());
        
        Set<InetAddress> seeds     = DatabaseDescriptor.getSeeds();
        Set<InetAddress> liveNodes = Gossiper.instance.getLiveMembers();

        //Pick a seed in the same DC as this node to be the job tracker
        for (InetAddress seed : seeds)           
            if (liveNodes.contains(seed) && DatabaseDescriptor.getEndpointSnitch().getDatacenter(seed).equals(localDC))               
                return seed;
        
        //Sleep a bit while gossip kicks in
        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        
        liveNodes = Gossiper.instance.getLiveMembers();

        for (InetAddress seed : seeds)           
            if (liveNodes.contains(seed) && DatabaseDescriptor.getEndpointSnitch().getDatacenter(seed).equals(localDC))
                return seed;
        
        throw new RuntimeException("No live seeds found in this DC: "+localDC);
    }   
}
