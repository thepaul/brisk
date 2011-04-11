package org.apache.cassandra.hadoop.hive.metastore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStorePersister
{
    private static final Logger log = LoggerFactory.getLogger(MetaStorePersister.class);
    
    private TSerializer serializer;
    private TDeserializer deserializer;
    private Cassandra.Iface client;
    
    public MetaStorePersister(Cassandra.Iface client) 
    {
        this.client = client;
    }

    @SuppressWarnings("unchecked")
    public void save(Map<? extends TFieldIdEnum, FieldMetaData> metaData,
            TBase base) throws CassandraHiveMetaStoreException
    {
        serializer = new TSerializer();
        Database db = (Database) base;
        BatchMutation batchMutation = new BatchMutation();
        if ( log.isDebugEnabled() )
            log.debug("class: {} dbname: {}", base.getClass().getName(), db.getName());
        try
        {
            batchMutation.addInsertion(ByteBufferUtil.bytes(db.getName()), Arrays.asList("MetaStore"), 
                    new Column(
                    ByteBufferUtil.bytes(base.getClass().getName()),
                    ByteBuffer.wrap(serializer.serialize(base)), System
                            .currentTimeMillis() * 1000));
            
            client.set_keyspace("HiveMetaStore");
            client.batch_mutate(batchMutation.getMutationMap(),
                    ConsistencyLevel.QUORUM);
        } 
        catch (Exception e)
        {
            // TODO add exception handling wrapper
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }

    }
    
    @SuppressWarnings("unchecked")
    public TBase load(Class clazz, String databaseName) 
        throws CassandraHiveMetaStoreException
    {
        log.debug("class: {} dbname: {}", clazz.getName(), databaseName);
        deserializer = new TDeserializer();
        String colName = clazz.getName();
        TBase base;
        try 
        {
            base = (TBase)clazz.newInstance();
            client.set_keyspace("HiveMetaStore");
            ColumnPath columnPath = new ColumnPath("MetaStore");
            columnPath.setColumn(ByteBufferUtil.bytes(colName));
            ColumnOrSuperColumn cosc = client.get(ByteBufferUtil.bytes(databaseName), columnPath, ConsistencyLevel.QUORUM);
            deserializer.deserialize(base, cosc.getColumn().getValue());
        } 
        catch (Exception e) 
        {
            // TODO same exception handling wrapper as above
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }
                
        return base;
    }
}
