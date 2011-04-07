package org.apache.cassandra.hadoop.hive.metastore;

/**
 * CassandraHiveMetaStore specific exception. Usually the result of
 * communication issues with the cluster.
 * 
 * @author zznate
 */
public class CassandraHiveMetaStoreException extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    private static final String DEF_MSG = "There was a problem with the Cassandra Hive MetaStore: ";

    public CassandraHiveMetaStoreException()
    {
        super(DEF_MSG);
    }

    public CassandraHiveMetaStoreException(String msg)
    {
        super(DEF_MSG + msg);
    }

    public CassandraHiveMetaStoreException(String msg, Throwable t)
    {
        super(DEF_MSG + msg, t);
    }
}
