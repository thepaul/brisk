package org.apache.cassandra.hadoop.trackers;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Comparator;
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

    //Will pick a seed to use as a job tracker in this local dc
    //We can't check for live seeds because if this is a ec2 cluster
    //the seeds might not be up yet :(
    public static InetAddress getJobTrackerNode()
    {       
        //Get this nodes local DC
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getLocalAddress());
        
        Set<InetAddress> seeds    = DatabaseDescriptor.getSeeds();
        
        
        InetAddress[] sortedSeeds = seeds.toArray(new InetAddress[]{});
        Arrays.sort(sortedSeeds, new Comparator<InetAddress>(){
            public int compare(InetAddress a, InetAddress b)
            {
                return a.getHostAddress().compareTo(b.getHostAddress());            
            }         
        }); 
        

        //Pick a seed in the same DC as this node to be the job tracker
        for (InetAddress seed : sortedSeeds)           
            if (DatabaseDescriptor.getEndpointSnitch().getDatacenter(seed).equals(localDC))
            {
                logger.info("Chose seed "+seed.getHostAddress()+" as jobtracker");
                return seed;
            } 
        
        throw new RuntimeException("No seeds found in this DC: "+localDC);
    }   
}
