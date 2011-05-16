package org.apache.cassandra.hadoop.hive.metastore;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;


public class SchemaManagerServiceTest extends MetaStoreTestBase
{
    private CassandraClientHolder cassandraClientHolder;
    private Configuration configuration;
    private SchemaManagerService schemaManagerService;
    
    
    public void setupLocal() throws Exception 
    {
        configuration = buildConfiguration();
        cassandraClientHolder = new CassandraClientHolder(configuration);
        schemaManagerService = new SchemaManagerService(cassandraClientHolder, configuration);
    }
    
    @Test
    public void testMetaStoreSchema() throws Exception 
    {
        setupLocal();
        boolean created = schemaManagerService.createMetaStoreIfNeeded();
        assertTrue(created);
        created = schemaManagerService.createMetaStoreIfNeeded();
        assertFalse(created);
    }

}
