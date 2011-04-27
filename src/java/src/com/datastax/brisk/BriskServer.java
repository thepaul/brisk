package com.datastax.brisk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableReader.Operator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MappedFileDataInput;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class BriskServer extends CassandraServer implements Brisk.Iface
{
    
    static final Logger     logger         = Logger.getLogger(BriskServer.class);

    static final String     cfsKeyspace    = "cfs";
    static final String     cfsInodeFamily = "inode";
    static final String     cfsBlockFamily = "sblocks";
    
    static final ByteBuffer dataCol        = ByteBufferUtil.bytes("data");
    static final ColumnParent blockDataPath= new ColumnParent(cfsBlockFamily);
    static final QueryPath    inodeQueryPath =  new QueryPath(cfsInodeFamily, null, dataCol);

    public LocalOrRemoteBlock get_cfs_block(String callerHostName, ByteBuffer blockId, int offset) throws TException, TimedOutException, UnavailableException, InvalidRequestException, NotFoundException
    {

        // This logic is only used on mmap spec machines
        if (DatabaseDescriptor.getDiskAccessMode() == DiskAccessMode.mmap)
        {
            
            logger.info("Checking for local block: "+blockId+" from "+callerHostName+" on "+FBUtilities.getLocalAddress().getHostName() );
            
            List<String> hosts = getKeyLocations(blockId);
            boolean isLocal = false;
            
            for (String hostName : hosts)
            {
                logger.info("Block "+blockId+" lives on "+hostName);
                
                if (hostName.equals(callerHostName) && hostName.equals(FBUtilities.getLocalAddress().getHostName()))
                {
                    isLocal = true;            
                    
                    break;
                }
            }
            
            if(isLocal)
            {
                logger.info("Local block should be on this node "+blockId);
                
                LocalBlock localBlock = getLocalBlock(blockId, offset);               
                
                if(localBlock != null)
                {
                    logger.info("Local block found: "+localBlock);
                    
                    return new LocalOrRemoteBlock().setLocal_block(localBlock);
                }
            }       
        }
        
        logger.info("Checking for remote block: "+blockId);
       
        //Fallback to storageProxy
        return getRemoteBlock(blockId, offset);
        
    }

    public List<List<String>> describe_keys(String keyspace, List<ByteBuffer> keys) throws TException
    {
        List<List<String>> keyEndpoints = new ArrayList<List<String>>(keys.size());

        for (ByteBuffer key : keys)
        {
            keyEndpoints.add(getKeyLocations(key));
        }

        return keyEndpoints;
    }

    private List<String> getKeyLocations(ByteBuffer key)
    {
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(cfsKeyspace, key);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

        List<String> hosts = new ArrayList<String>(endpoints.size());

        for (InetAddress endpoint : endpoints)
        {
            hosts.add(endpoint.getHostName());
        }

        return hosts;
    }

    private LocalBlock getLocalBlock(ByteBuffer blockId, int offset) throws TException
    {

        DecoratedKey<Token<?>> decoratedKey = new DecoratedKey<Token<?>>(StorageService.getPartitioner().getToken(
                blockId), blockId);

        Table table = Table.open(cfsKeyspace);
        ColumnFamilyStore blockStore = table.getColumnFamilyStore(cfsBlockFamily);

        Collection<SSTableReader> sstables = blockStore.getSSTables();

        for (SSTableReader sstable : sstables)
        { 
            
            long position = sstable.getPosition(decoratedKey, Operator.EQ);

            if (position == -1)
                continue;

            String filename = sstable.descriptor.filenameFor(Component.DATA);
            RandomAccessFile raf = null;
            int mappedLength = -1;
            MappedByteBuffer mappedData = null;
            MappedFileDataInput file = null;
            try
            {
                
                raf = new RandomAccessFile(filename, "r");
                assert position < raf.length();
                
                mappedLength = (raf.length() - position) < Integer.MAX_VALUE ? (int) (raf.length() - position)
                        : Integer.MAX_VALUE;

                mappedData = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, mappedLength);

                file = new MappedFileDataInput(mappedData, filename, 0);

                if (file == null)
                    continue;
                
                //Verify key was found in data file
                DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                        sstable.descriptor,
                        ByteBufferUtil.readWithShortLength(file));
                assert keyInDisk.equals(decoratedKey) : String.format("%s != %s in %s", keyInDisk, decoratedKey, file.getPath());
                
                long rowSize = SSTableReader.readRowSize(file, sstable.descriptor);

                assert rowSize > 0;
                assert rowSize < mappedLength;

                IndexHelper.skipBloomFilter(file);
                IndexHelper.skipIndex(file);

                // we can stop early if bloom filter says none of the
                // columns actually exist -- but,
                // we can't stop before initializing the cf above, in
                // case there's a relevant tombstone
                ColumnFamilySerializer serializer = ColumnFamily.serializer();
                try
                {
                    ColumnFamily cf = serializer.deserializeFromSSTableNoColumns(ColumnFamily.create(sstable.metadata),
                            file);

                    if (cf.isMarkedForDelete())
                        continue;

                }
                catch (Exception e)
                {
                    throw new IOException(serializer + " failed to deserialize " + sstable.getColumnFamilyName()
                            + " with " + sstable.metadata + " from " + file, e);
                }

                // verify column count
                int numColumns = file.readInt();
                assert numColumns == 1;

                // verify column name
                ByteBuffer name = ByteBufferUtil.readWithShortLength(file);
                assert name.equals(dataCol);

                // verify column type;
                int b = file.readUnsignedByte();
                if ((b & ColumnSerializer.DELETION_MASK) != 0 || (b & ColumnSerializer.EXPIRATION_MASK) != 0)
                {
                    continue;
                }

                // skip ts
                long ts = file.readLong();
                int blockLength = file.readInt();

                int bytesReadFromStart = mappedLength - (int)file.bytesRemaining();

                logger.info("BlockLength = "+blockLength+" Availible "+file.bytesRemaining());
                
                assert offset <= blockLength : String.format("%d > %d", offset,  blockLength);

                long dataOffset = position + bytesReadFromStart;
                
                if(file.bytesRemaining() == 0 || blockLength == 0)
                    return null;
                

                return new LocalBlock(file.getPath(), dataOffset + offset, blockLength - offset);

            }
            catch (IOException e)
            {
                throw new TException(e);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }
        }

        
        return null;
    }

    private LocalOrRemoteBlock getRemoteBlock(ByteBuffer blockId, int offset) throws TimedOutException, UnavailableException, InvalidRequestException, NotFoundException
    {
        ReadCommand rc = new SliceByNamesReadCommand(cfsKeyspace, blockId, blockDataPath, Arrays.asList(dataCol));
             
        try
        {
            List<Row> rows = StorageProxy.read(Arrays.asList(rc), ConsistencyLevel.ONE);
            
            if(rows.isEmpty())
                throw new NotFoundException();
            
            if(rows.size() > 1)
                throw new RuntimeException("Block id returned more than one row");
            
            Row row = rows.get(0);
            if(row.cf == null)
                throw new NotFoundException();
            
            IColumn col = row.cf.getColumn(dataCol);
            
            if(col == null || !col.isLive())
                throw new NotFoundException();
            
            
            ByteBuffer value = col.value();
            
            if(value.remaining() < offset)
                throw new InvalidRequestException("Invalid offset for block of size: "+value.remaining());
            
            
            LocalOrRemoteBlock block = new LocalOrRemoteBlock();
            if(offset > 0)
            {
                ByteBuffer offsetBlock = value.duplicate();
                offsetBlock.position(offsetBlock.position()+offset);
                block.setRemote_block(offsetBlock);
            }   
            else
            {
                block.setRemote_block(value);
            }
            
            return block;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
    }
    
}
