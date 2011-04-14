package org.apache.cassandra;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class EmbeddedBriskErrorServer
{
    protected static BriskErrorDaemon daemon = null;
    
    static ExecutorService executor = Executors.newSingleThreadExecutor();
    
    public static void startBrisk() throws IOException

    {
        executor.execute(new Runnable()
        {
            public void run()
            {
                daemon = new BriskErrorDaemon();
               
                daemon.activate();
            }
        });
        try
        {
            TimeUnit.SECONDS.sleep(3);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }
    
    public static void stopBrisk() throws Exception
    {
        if (daemon != null)
        {
            daemon.deactivate();
        }
        executor.shutdown();
        executor.shutdownNow();
    }
}
