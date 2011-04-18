package org.apache.cassandra.hadoop.hive.metastore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
    
    @Test
    public void testCreateDeleteDatabaseAndTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("db_name");
        assertEquals(database, foundDatabase);
        
        Table table = new Table();
        table.setDbName("db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("db_name", "table_name");
        assertEquals(table, foundTable);
        
        assertEquals(1,metaStore.getAllDatabases().size());
        assertEquals(1,metaStore.getAllTables("db_name").size());
        
        metaStore.dropTable("db_name", "table_name");
        assertNull(metaStore.getTable("db_name", "table_name"));
        assertEquals(0,metaStore.getAllTables("db_name").size());
    }
    
    @Test
    public void testFindEmptyPatitionList() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("db_name");
        assertEquals(database, foundDatabase);
        
        Table table = new Table();
        table.setDbName("db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        List<String> partitionNames = metaStore.listPartitionNames("db_name", "table_name", (short) 100);
        assertEquals(0, partitionNames.size());
        partitionNames = metaStore.listPartitionNames("db_name", "table_name", (short) -1);
        assertEquals(0, partitionNames.size());
    }
    
    @Test
    public void testAlterTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        
        Table table = new Table();
        table.setDbName("alter_table_db_name");
        table.setTableName("orig_table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("alter_table_db_name", "orig_table_name");
        assertEquals(table, foundTable);
        
        Table altered = new Table();
        altered.setDbName("alter_table_db_name");
        altered.setTableName("new_table_name");
               
        metaStore.alterTable("alter_table_db_name", "orig_table_name", altered);
        assertEquals(1,metaStore.getAllTables("alter_table_db_name").size());
        
    }
    
    @Test
    @Ignore
    public void testDatabaseTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("alter_db_db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        
        Table table = new Table();
        table.setDbName("alter_db_db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("alter_db_db_name", "table_name");
        assertEquals(table, foundTable);
        Database altered = new Database("alter_db_db_name2", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.alterDatabase("alter_db_db_name", altered);
        
        assertEquals(1,metaStore.getAllTables("alter_db_db_name2").size());
        
    }
    
    
    private Configuration buildConfiguration() 
    {
        Configuration conf = new Configuration();
        conf.set(CassandraClientHolder.CONF_PARAM_HOST, "localhost");
        conf.setInt(CassandraClientHolder.CONF_PARAM_PORT, DatabaseDescriptor.getRpcPort());
        conf.setBoolean(CassandraClientHolder.CONF_PARAM_FRAMED, true);
        conf.setBoolean(CassandraClientHolder.CONF_PARAM_RANDOMIZE_CONNECTIONS, true);
        return conf;
    }
}
