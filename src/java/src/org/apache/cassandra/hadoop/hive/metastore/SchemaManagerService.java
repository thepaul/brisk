package org.apache.cassandra.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.Random;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service encapsulating schema managment of the Brisk platform. This service deals 
 * with both the Hive meta store schema as well as maintaining the mappings of 
 * column family and keyspace objects to meta store tables and databases respectively.
 * 
 * @author zznate
 */
public class SchemaManagerService
{

    private static Logger log = LoggerFactory.getLogger(SchemaManagerService.class);
    private CassandraClientHolder cassandraClientHolder;
    private Configuration configuration;
    
    public SchemaManagerService(CassandraClientHolder cassandraClientHolder, Configuration configuration)
    {
        this.cassandraClientHolder = cassandraClientHolder;
        this.configuration = configuration;
    }
    
    /**
     * Create the meta store keyspace if it does not already exist.
     * @return true if the keyspace did no exist and the creation was successful. False if the 
     * keyspace already existed. 
     * @throws {@link CassandraHiveMetaStoreException} wrapping the underlying exception if we
     * failed to create the keyspace.
     */
    public boolean createMetaStoreIfNeeded() 
    {
        // Database_entities : {[name].name=name}
        // FIXME add these params to configuration
        // databaseName=metastore_db;create=true
        try 
        {
            cassandraClientHolder.applyKeyspace();
            return false;
        }
        catch (CassandraHiveMetaStoreException chmse) 
        {
            log.debug("Attempting to create meta store keyspace: First set_keyspace call failed. Sleeping.");                       
        }
        
        //Sleep a random amount of time to stagger ks creations on many nodes       
        try
        {
            Thread.sleep(new Random().nextInt(5000));
        }
        catch (InterruptedException e1)
        {
          
        }
                                
        //check again...
        try 
        {
            cassandraClientHolder.applyKeyspace();
            return false;
        }  
        catch (CassandraHiveMetaStoreException chmse) 
        {
            log.debug("Attempting to create meta store keyspace after sleep.");
        }
        
        CfDef cf = new CfDef(cassandraClientHolder.getKeyspaceName(), 
                cassandraClientHolder.getColumnFamily());
        cf.setKey_validation_class("UTF8Type");
        cf.setComparator_type("UTF8Type");
        KsDef ks = new KsDef(cassandraClientHolder.getKeyspaceName(), 
                "org.apache.cassandra.locator.SimpleStrategy",  
                Arrays.asList(cf));
        ks.setStrategy_options(KSMetaData.optsWithRF(configuration.getInt(CassandraClientHolder.CONF_PARAM_REPLICATION_FACTOR, 1)));
        try 
        {
            cassandraClientHolder.getClient().system_add_keyspace(ks);
            return true;
        } 
        catch (Exception e) 
        {
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }    
    }
    
}
