/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

/**
 * A facility for storing and retrieving {@link INode}s and {@link Block}s.
 */
public interface CassandraFileSystemStore
{

    void initialize(URI uri, Configuration conf) throws IOException;

    String getVersion() throws IOException;

    void storeINode(Path path, INode inode) throws IOException;

    void storeBlock(Block block, ByteArrayOutputStream file) throws IOException;

    INode retrieveINode(Path path) throws IOException;

    InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException;

    void deleteINode(Path path) throws IOException;

    void deleteBlock(Block block) throws IOException;

    Set<Path> listSubPaths(Path path) throws IOException;

    Set<Path> listDeepSubPaths(Path path) throws IOException;

    BlockLocation[] getBlockLocation(List<Block> usedBlocks, long start, long len) throws IOException;
}