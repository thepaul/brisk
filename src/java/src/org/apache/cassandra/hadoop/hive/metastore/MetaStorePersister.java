package org.apache.cassandra.hadoop.hive.metastore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generically persist and load the Hive Meta Store model classes
 * 
 * @author zznate
 *
 */
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
            TBase base, String databaseName) throws CassandraHiveMetaStoreException
    {
        // TODO need to add ID field to column name lookup to avoid overwrites
        serializer = new TSerializer();        
        BatchMutation batchMutation = new BatchMutation();
        if ( log.isDebugEnabled() )
            log.debug("class: {} dbname: {}", base.getClass().getName(), databaseName);
        try
        {
            batchMutation.addInsertion(ByteBufferUtil.bytes(databaseName), Arrays.asList("MetaStore"), 
                    new Column(
                    ByteBufferUtil.bytes(buildEntityColumnName(base)),
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
    public TBase load(TBase base, String databaseName) 
        throws CassandraHiveMetaStoreException
    {
        if ( log.isDebugEnabled() )
            log.debug("class: {} dbname: {}", base.getClass().getName(), databaseName);
        deserializer = new TDeserializer();        
        try 
        {
            client.set_keyspace("HiveMetaStore");
            ColumnPath columnPath = new ColumnPath("MetaStore");
            columnPath.setColumn(ByteBufferUtil.bytes(buildEntityColumnName(base)));
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
    
    public List<TBase> find(TBase base, String databaseName) 
    throws CassandraHiveMetaStoreException
    {
        return find(base, databaseName, null, 100);
    }
    
    @SuppressWarnings("unchecked")
    public List<TBase> find(TBase base, String databaseName, String prefix, int count) 
        throws CassandraHiveMetaStoreException
    {
        if ( log.isDebugEnabled() )
            log.debug("class: {} dbname: {}", base.getClass().getName(), databaseName);
        deserializer = new TDeserializer();
        
        List<TBase> resultList;
        try 
        {
            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(buildEntitySlicePrefix(base, prefix, count));
            client.set_keyspace("HiveMetaStore");
            List<ColumnOrSuperColumn> cols = client.get_slice(ByteBufferUtil.bytes(databaseName), 
                    new ColumnParent("MetaStore"), 
                    predicate, 
                    ConsistencyLevel.ONE);
            resultList = new ArrayList<TBase>(cols.size());
            for (ColumnOrSuperColumn cosc : cols)
            {
                TBase other = base.deepCopy();
                deserializer.deserialize(other, cosc.getColumn().getValue());
                resultList.add(other);
            }             
        } 
        catch (Exception e) 
        {
            // TODO same exception handling wrapper as above
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }                
        return resultList;
    }
    
    private String buildEntityColumnName(TBase base) {
        StringBuilder colName = new StringBuilder(96);
        colName.append(base.getClass().getName()).append("::");
        if ( base instanceof Table ) 
        {
            colName.append(((Table)base).getTableName());
        } else if ( base instanceof Index ) 
        {
            colName.append(((Index)base).getOrigTableName())
            .append("::")
            .append(((Index)base).getIndexTableName())
            .append("::")
            .append(((Index)base).getIndexName());
        } else if ( base instanceof Partition ) 
        {
            colName.append(((Partition)base).getTableName());
            for( String value : ((Partition)base).getValues()) 
            {
                colName.append("::").append(value);
            }
        } else if ( base instanceof Type )
        {
            colName.append(((Type)base).getName());            
        } else if ( base instanceof Role)
        {
            colName.append(((Role)base).getRoleName());
        } 
        if ( log.isDebugEnabled() )
            log.debug("Constructed columnName: {}", colName);
        return colName.toString();
    }
    
    /**
     * Construct a basic slice op, since we do most of our filtering client side
     *     
     * @param base the object we use for the column name
     * @param prefix the prefix to prepend. Can be null.
     * @param count the number of items to which we restrict this predicate
     */
    private SliceRange buildEntitySlicePrefix(TBase base, String prefix, int count) {
        StringBuilder colName = new StringBuilder(96);
        colName.append(base.getClass().getName()).append("::");
        if ( prefix != null && !prefix.isEmpty() ) 
            colName.append(prefix);
        SliceRange sliceRange = new SliceRange(ByteBufferUtil.bytes(colName.toString()), 
                ByteBufferUtil.EMPTY_BYTE_BUFFER, false, count);
        if ( log.isDebugEnabled() )
            log.debug("Constructed columnName for slice: {}", colName);
        return sliceRange;
    }
}
