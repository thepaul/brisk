package org.apache.cassandra.hadoop.hive.metastore;

import java.io.IOException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetaStorePersisterTest extends CleanupHelper
{

    private MetaStorePersister metaStorePersister = new MetaStorePersister();
    
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        EmbeddedServer.startBrisk();                
    }
    
    @Test
    public void testBasicPersistMetaStoreEntity() 
    {
        Database database = new Database();
        metaStorePersister.save(database.metaDataMap, database); // save(TBase base).. via _Fields and findByThriftId, publi MetaDataMap
    }
}
