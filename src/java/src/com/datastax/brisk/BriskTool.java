package com.datastax.brisk;

import java.io.IOException;

import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;


/**
 * Command line tool for brisk specific commands
 */
public class BriskTool
{
    
    private int port = 9160;
    private String host = "localhost";
    
    private enum Commands {
        jobtracker
    }
    
    public BriskTool(String[] args)
    {
        if (args.length == 0)
            usage();

        // parse args
        for (int i = 0; i < args.length-1; i++) {
            
            if (args[i].startsWith("--")) {
                int eq = args[i].indexOf("=");

                if (eq < 0)
                    usage();

                String arg = args[i].substring(2, eq);
                String value = args[i].substring(eq + 1);

                try {
                    if (arg.equalsIgnoreCase("host"))
                        host = value;

                    if (arg.equalsIgnoreCase("port"))
                        port = Integer.valueOf(value);
                                                         
                } catch (Throwable t) {
                    usage();
                }
            }
        }
        
        
        
        //process command
        Commands cmd = null;
        try
        {
            cmd = Commands.valueOf(args[args.length -1]);
        }
        catch(Throwable t)
        {
            usage();
        }
        
        try
        {
            runCommand(cmd);
        }
        catch (IOException e)
        {
            System.err.println("Unknown exception when running command: ");
            e.printStackTrace();
            System.exit(2);
        }
        
    }
                
    private void usage()
    {
        System.err.print(BriskTool.class.getSimpleName() + " [--host=<hostname>] [--port=<#>] cmd\n"
                + "  Commands:\n"
                + "\tjobtracker     returns the jobtracker hostname and port\n");

        System.exit(1);
    }
    
    private Brisk.Iface getConnection() throws IOException
    {
        TSocket socket = new TSocket(host, port);
        TTransport trans = new TFramedTransport(socket);
        try
        {
            trans.open();
        }
        catch (TTransportException e)
        {
            throw new IOException("unable to connect to server", e);
        }

        Brisk.Iface client = new Brisk.Client(new TBinaryProtocol(trans));

        return client;
        
    }
    
    private void runCommand(Commands cmd) throws IOException
    {
        Brisk.Iface client = getConnection(); 
        
        switch(cmd)
        {
        case jobtracker:
           getJobTracker(client); break;
        default:
            throw new IllegalStateException("no handler for command: "+cmd);
        }        
    }
    
    private void getJobTracker(Brisk.Iface client)
    {
        try
        {
            System.out.println(client.get_jobtracker_address());
        }catch(NotFoundException e)
        {
            System.err.println("No jobtracker found");
            System.exit(2);
        }
        catch (TException e)
        {
            System.err.println("Error when fetching jobtracker address: "+e);
            System.exit(2);
        }
    }
    
    public static void main(String args[])
    {
        new BriskTool(args);
    }
    
}
