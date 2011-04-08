package org.apache.cassandra.hadoop.hive.metastore;

import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStorePersister
{
    private static final Logger log = LoggerFactory.getLogger(MetaStorePersister.class);
    
    public void save(Map<? extends TFieldIdEnum,FieldMetaData> metaData, 
            TBase<? extends TBase, ? extends TFieldIdEnum> base) 
    {
        for (Map.Entry<? extends TFieldIdEnum,FieldMetaData> entry : metaData.entrySet() )
        {
            FieldMetaData fmd = entry.getValue();
            log.info("FieldMetaData: {}", fmd);
            //fmd.fieldName
        }
    }
}
