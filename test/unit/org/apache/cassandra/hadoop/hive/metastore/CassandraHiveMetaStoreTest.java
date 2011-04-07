package org.apache.cassandra.hadoop.hive.metastore;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test plumbing of CassandraHiveMetaStore
 * 
 * @author zznate
 */
public class CassandraHiveMetaStoreTest extends CleanupHelper {


    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        EmbeddedServer.startBrisk();                
    }

    @Test
    public void testSetConf() 
    {
        // INIT procedure from HiveMetaStore:
        //empty c-tor
        // setConf
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        // Throw a runtime exception if we can ping cassandra?
        metaStore.setConf(buildConfiguration());        
    }
    
    @Test(expected=CassandraHiveMetaStoreException.class)
    public void testSetConfConnectionFail() 
    {
        // INIT procedure from HiveMetaStore:
        //empty c-tor
        // setConf
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        // Throw a runtime exception if we can ping cassandra?
        Configuration conf = buildConfiguration();
        conf.setInt("cassandra.connection.port",8181);
        metaStore.setConf(conf);        
    }
    
    private Configuration buildConfiguration() {
        Configuration conf = new Configuration();
        conf.set(CassandraHiveMetaStore.CONF_PARAM_HOST, "localhost");
        conf.setInt(CassandraHiveMetaStore.CONF_PARAM_PORT, DatabaseDescriptor.getRpcPort());
        conf.setBoolean(CassandraHiveMetaStore.CONF_PARAM_FRAMED, true);
        conf.setBoolean(CassandraHiveMetaStore.CONF_PARAM_RANDOMIZE_CONNECTIONS, true);
        return conf;
    }
}
