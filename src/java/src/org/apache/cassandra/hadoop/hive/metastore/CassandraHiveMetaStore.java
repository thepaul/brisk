package org.apache.cassandra.hadoop.hive.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraHiveMetaStore implements RawStore {        
    
    private static final Logger log = LoggerFactory.getLogger(CassandraHiveMetaStore.class);
    private Configuration configuration;
    private Cassandra.Iface client;
    private MetaStorePersister metaStorePersister;
    
    public CassandraHiveMetaStore()
    {
        log.debug("Creating CassandraHiveMetaStore");
    }
    

    public void setConf(Configuration conf)
    {
        configuration = conf;
        try {
            // cassandra.connection.[host|port|framed|randomizeConnections]
            // anything else needed?
            client = CassandraProxyClient.newProxyConnection(conf.get(CONF_PARAM_HOST, "localhost"), 
                    conf.getInt(CONF_PARAM_PORT, 9160), 
                    conf.getBoolean(CONF_PARAM_FRAMED,true), 
                    conf.getBoolean(CONF_PARAM_RANDOMIZE_CONNECTIONS, true));
            metaStorePersister = new MetaStorePersister(client);
        } 
        catch (IOException ioe) 
        {
            throw new CassandraHiveMetaStoreException("Could not connect to Cassandra. Reason: " + ioe.getMessage(), ioe);
        }                
    }
    
    public Configuration getConf()
    {        
        return configuration;
    }    

    
    public void createDatabase(Database database) throws InvalidObjectException, MetaException
    {
        log.debug("createDatabase with {}", database);
        // Database_entities : {[name].name=name}
        // FIXME add these params to configuration
        // databaseName=metastore_db;create=true
        CfDef cf = new CfDef("HiveMetaStore", "MetaStore");
        KsDef ks = new KsDef("HiveMetaStore", "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cf));
        try {
            client.system_add_keyspace(ks);
            metaStorePersister.save(database.metaDataMap, database, database.getName());
        } catch (Exception e) {
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }
    }

    
    public Database getDatabase(String database) throws NoSuchObjectException
    {
        log.debug("in getDatabase with database name: {}", database);
        try {
            Database db = new Database();
            metaStorePersister.load(db, database);
            return db;
        } catch (Exception e) {
            // TODO fancier exception catching mechanism
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }        
    }
    
    public void createTable(Table table) throws InvalidObjectException, MetaException
    {
        try {            
            metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        } catch (Exception e) {
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }
    }

    public Table getTable(String databaseName, String tableName) throws MetaException
    {
        log.debug("in getTable with database name: {} and table name: {}", databaseName, tableName);
        try {
            Table table = new Table();
            metaStorePersister.load(table, databaseName);
            return table;
        } catch (Exception e) {
            throw new CassandraHiveMetaStoreException("Could not create Hive MetaStore database: " + e.getMessage(), e);
        }
    }

    
    public boolean addIndex(Index arg0) throws InvalidObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean addPartition(Partition arg0) throws InvalidObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean addRole(String arg0, String arg1)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean alterDatabase(String arg0, Database arg1)
            throws NoSuchObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void alterIndex(String arg0, String arg1, String arg2, Index arg3)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void alterPartition(String arg0, String arg1, Partition arg2)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void alterTable(String arg0, String arg1, Table arg2)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean commitTransaction()
    {
        // TODO Auto-generated method stub
        return false;
    }



    @Override
    public boolean createType(Type arg0)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dropDatabase(String arg0) throws NoSuchObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dropIndex(String arg0, String arg1, String arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dropPartition(String arg0, String arg1, List<String> arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dropTable(String arg0, String arg1) throws MetaException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dropType(String arg0)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getAllTables(String arg0) throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, String arg4,
            List<String> arg5) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String arg0, String arg1,
            List<String> arg2) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public List<String> getDatabases(String catalog) throws MetaException
    {
        log.debug("in getDatabases with catalog: {}", catalog);
        return null;
    }

    @Override
    public Index getIndex(String arg0, String arg1, String arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Index> getIndexes(String arg0, String arg1, int arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition getPartition(String arg0, String arg1, List<String> arg2)
            throws MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, List<String> arg4)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Partition getPartitionWithAuth(String arg0, String arg1,
            List<String> arg2, String arg3, List<String> arg4)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Partition> getPartitions(String arg0, String arg1, int arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Partition> getPartitionsByFilter(String arg0, String arg1,
            String arg2, short arg3) throws MetaException,
            NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String arg0, String arg1,
            short arg2, String arg3, List<String> arg4) throws MetaException,
            NoSuchObjectException, InvalidObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Role getRole(String arg0) throws NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String arg0, String arg1,
            String arg2, List<String> arg3) throws InvalidObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }


    public List<String> getTables(String dbName, String tableName)
            throws MetaException
    {
        log.info("in getTables with dbName: {} and tableName: {}", dbName, tableName);
        return null;
    }

    @Override
    public Type getType(String arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String arg0,
            List<String> arg1) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean grantRole(Role arg0, String arg1, PrincipalType arg2,
            String arg3, PrincipalType arg4, boolean arg5)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<MTablePrivilege> listAllTableGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listIndexNames(String arg0, String arg1, short arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listPartitionNames(String arg0, String arg1, short arg2)
            throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listPartitionNamesByFilter(String arg0, String arg1,
            String arg2, short arg3) throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MDBPrivilege> listPrincipalDBGrants(String arg0,
            PrincipalType arg1, String arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MGlobalPrivilege> listPrincipalGlobalGrants(String arg0,
            PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4, String arg5)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionPrivilege> listPrincipalPartitionGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3, String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listRoleNames()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MRoleMap> listRoles(String arg0, PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean openTransaction()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeRole(String arg0) throws MetaException,
            NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean revokeRole(Role arg0, String arg1, PrincipalType arg2)
            throws MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void rollbackTransaction()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown()
    {
        // TODO Auto-generated method stub

    }

    // TODO are there more appropriate hive config parameters to use here?
    
    private static final String CONF_PARAM_PREFIX = "cassandra.connection.";
    /** Initial Apache Cassandra node to which we will connect (localhost) */
    public static final String CONF_PARAM_HOST = CONF_PARAM_PREFIX+"host";
    /** Thrift port for Apache Cassandra (9160) */
    public static final String CONF_PARAM_PORT = CONF_PARAM_PREFIX+"port";
    /** Boolean indicating use of Framed vs. Non-Framed Thrift transport (true) */
    public static final String CONF_PARAM_FRAMED = CONF_PARAM_PREFIX+"framed";
    /** Pick a host at random from the ring as opposed to using the same host (true) */
    public static final String CONF_PARAM_RANDOMIZE_CONNECTIONS = CONF_PARAM_PREFIX+"randomizeConnections";

}
