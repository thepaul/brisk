/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Charsets;

import org.junit.BeforeClass;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.IndexType;

public class SchemaLoader
{
    @BeforeClass
    public static void loadSchema()
    {
        try
        {
            for (KSMetaData ksm : schemaDefinition())
            {
                for (CFMetaData cfm : ksm.cfMetaData().values())
                    CFMetaData.map(cfm);
                DatabaseDescriptor.setTableDefinition(ksm, DatabaseDescriptor.getDefsVersion());
            }
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Collection<KSMetaData> schemaDefinition()
    {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();

        // A whole bucket of shorthand
        String ks1 = "Keyspace1";
        String ks2 = "Keyspace2";
        String ks3 = "Keyspace3";
        String ks4 = "Keyspace4";
        String ks5 = "Keyspace5";
        String ks_kcs = "KeyCacheSpace";
        String ks_rcs = "RowCacheSpace";

        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        Map<String, String> opts_rf1 = KSMetaData.optsWithRF(1);
        Map<String, String> opts_rf2 = KSMetaData.optsWithRF(2);
        Map<String, String> opts_rf3 = KSMetaData.optsWithRF(3);
        Map<String, String> opts_rf5 = KSMetaData.optsWithRF(5);

        ColumnFamilyType st = ColumnFamilyType.Standard;
        ColumnFamilyType su = ColumnFamilyType.Super;
        AbstractType bytes = BytesType.instance;

        // Keyspace 1
        schema.add(new KSMetaData(
                ks1,
                simple,
                opts_rf1,

                // Column Families
                standardCFMD(ks1, "Standard1"), standardCFMD(ks1, "Standard2"), standardCFMD(ks1, "Standard3"),
                standardCFMD(ks1, "Standard4"), standardCFMD(ks1, "StandardLong1"), standardCFMD(ks1, "StandardLong2"),
                superCFMD(ks1, "Super1", LongType.instance), superCFMD(ks1, "Super2", LongType.instance), superCFMD(
                        ks1, "Super3", LongType.instance), superCFMD(ks1, "Super4", UTF8Type.instance), superCFMD(ks1,
                        "Super5", bytes), indexCFMD(ks1, "Indexed1", true), indexCFMD(ks1, "Indexed2", false),
                new CFMetaData(ks1, "StandardInteger1", st, IntegerType.instance, null).keyCacheSize(0),
                new CFMetaData(ks1, "Counter1", st, bytes, null).defaultValidator(CounterColumnType.instance),
                new CFMetaData(ks1, "SuperCounter1", su, bytes, bytes).defaultValidator(CounterColumnType.instance),
                jdbcCFMD(ks1, "JdbcInteger", IntegerType.instance), jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance),
                jdbcCFMD(ks1, "JdbcLong", LongType.instance), jdbcCFMD(ks1, "JdbcBytes", bytes), jdbcCFMD(ks1,
                        "JdbcAscii", AsciiType.instance)));


        return schema;
    }

    private static CFMetaData standardCFMD(String ksName, String cfName)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, BytesType.instance, null).keyCacheSize(0);
    }

    private static CFMetaData superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Super, BytesType.instance, subcc).keyCacheSize(0);
    }

    private static CFMetaData indexCFMD(String ksName, String cfName, final Boolean withIdxType)
    {
        return standardCFMD(ksName, cfName).columnMetadata(
                Collections.unmodifiableMap(new HashMap<ByteBuffer, ColumnDefinition>() {
                    {
                        ByteBuffer cName = ByteBuffer.wrap("birthdate".getBytes(Charsets.UTF_8));
                        IndexType keys = withIdxType ? IndexType.KEYS : null;
                        put(cName, new ColumnDefinition(cName, LongType.instance, keys, null));
                    }
                }));
    }

    private static CFMetaData jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, comp, comp).defaultValidator(comp);
    }
}
