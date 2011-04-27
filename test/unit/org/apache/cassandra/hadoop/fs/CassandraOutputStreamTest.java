/**
 * 
 */
package org.apache.cassandra.hadoop.fs;


import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the logic to write into Blocks and SubBlocks.
 * 
 * @author patricioe (Patricio Echague - patricio@datastax.com)
 *
 */
public class CassandraOutputStreamTest extends CleanupHelper
{
	
	private final static Logger         logger        = Logger.getLogger(CassandraOutputStreamTest.class);
	
    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {
        EmbeddedServer.startBrisk();
    }
	
	private CassandraOutputStream out;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}
	
	/**
	 * Test that the SubBlock is not bigger than the Block.
	 * @throws Exception
	 */
	@Test
	public void testBlockSizeSmallerThanSubBlockSize() throws Exception {
		try {
			new CassandraOutputStream(null, null, null, null, 10, 20, null, 30);
			fail();
		} catch (IllegalArgumentException e) {
			// OK!
		}
	}
	
	/**
	 * Writes several bytes until generating an overflow so that we can test the end and beginning of the new block.
	 * BlockSize used 256 bytes.
	 * 
	 */
	@Test
	public void testWrite1() throws Exception {
		int blockSize = 2;
		int subblockSize = 2;
		int bufferSize = 100;
		int totalBytesToWrite = 8;
		int storedSubBlockesExpectation = 4; // 8 bytes into 2 bytes subbLock size => 4
		
		testWriteWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
		testWriteBufferWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
	}
	
	@Test
	public void testWrite2() throws Exception {
		int blockSize = 2;
		int subblockSize = 1;
		int bufferSize = 100;
		int totalBytesToWrite = 8;
		int storedSubBlockesExpectation = 8; // 8 bytes into 1 bytes subbLock size => 8
		
		testWriteWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
		testWriteBufferWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
	}
	
	@Test
	public void testWrite3() throws Exception {
		int blockSize = 2;
		int subblockSize = 1;
		int bufferSize = 100;
		int totalBytesToWrite = 9;
		int storedSubBlockesExpectation = 9; // 8 bytes into 1 bytes subbLock size + 1 extra byte => 9
		
		testWriteWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
		testWriteBufferWith(blockSize, subblockSize, bufferSize, totalBytesToWrite, storedSubBlockesExpectation);
	}
	
	/**
	 * Test CassandraOutputStream.write(int);
	 */
	private void testWriteWith(int blockSize, int subblockSize, int bufferSize,
			int totalBytesToWrite, int storedSubBlockesExpectation) throws Exception {
		
		StoreMock storeMock = new StoreMock();
		out = new CassandraOutputStream(null, storeMock, null, null, blockSize, subblockSize, null, bufferSize);
		
		Assert.assertEquals(0, out.getPos());
		
		for (int i = 0; i < totalBytesToWrite; i++) {
			out.write(i);
		}
		
		Assert.assertEquals(totalBytesToWrite, out.getPos());

		out.close();

		// Validate the expectations.
		Assert.assertEquals(storedSubBlockesExpectation, storeMock.storeSubBlockCount);
		
		// This is always one.
		Assert.assertEquals(1, storeMock.storeINodeCount);
		
		int totalBlocks = calculateTotalBlocks(totalBytesToWrite, blockSize);
		
		// Assert the total blocks per file
		Assert.assertEquals(totalBlocks, storeMock.inodesStored.get(0).getBlocks().length);
		
		// Assert SubBlocks per Block
		int totalSubBlocksPerBlock = blockSize % subblockSize == 0 ? blockSize / subblockSize : (blockSize / subblockSize) + 1;
		assertSubBlocksInBlocks(storeMock.inodesStored.get(0).getBlocks(), totalSubBlocksPerBlock, storedSubBlockesExpectation);
		
		// Assert and print for debug.
		for (Block block : storeMock.inodesStored.get(0).getBlocks()) {
			logger.info(block);
		}
	}

	/**
	 * Verify that the Blocks have the expected amount of SubBlocks.
	 */
	private void assertSubBlocksInBlocks(Block[] blocks, int totalSubBlocksPerBlock, int storedSubBlockesExpectation) {
		int totalSubBlocksSoFar = 0;
		for (Block block : blocks) {
			if (storedSubBlockesExpectation - totalSubBlocksSoFar < totalSubBlocksPerBlock)
			{
				// This is the last block. Assert the remaining subBlocks
				Assert.assertEquals(storedSubBlockesExpectation - totalSubBlocksSoFar , block.subBlocks.length);
			} else 
			{
				Assert.assertEquals(totalSubBlocksPerBlock , block.subBlocks.length);
			}
			
			// Keep accumulating
			totalSubBlocksSoFar += block.subBlocks.length;
		}
		
		// Validate the total Sub Blocks.
		Assert.assertEquals(storedSubBlockesExpectation, totalSubBlocksSoFar);
	}

	private int calculateTotalBlocks(int totalBytesToWrite, int blockSize) {
		return totalBytesToWrite % blockSize == 0 ? totalBytesToWrite / blockSize : (totalBytesToWrite / blockSize) + 1;
	}

	/**
	 * Test CassandraOutputStream.write(buffer, off, len);
	 */
	private void testWriteBufferWith(int blockSize, int subblockSize, int bufferSize,
			int totalBytesToWrite, int storedSubBlockesExpectation) throws Exception {
		
		// Null object here are not needed or irrelevant for this test case. 
		// buffer size different from bytes to write is intentional.
		StoreMock storeMock = new StoreMock();
		out = new CassandraOutputStream(null, storeMock, null, null, blockSize, subblockSize, null, bufferSize);
		
		Assert.assertEquals(0, out.getPos());
		
		// Fill up the buffer
		byte[] buffer = new byte[totalBytesToWrite];
		for (int i = 0; i < totalBytesToWrite; i++) {
			buffer[i] = (byte) i;
		}
		
		// Invoke the method being tested.
		out.write(buffer, 0, totalBytesToWrite);
		
		Assert.assertEquals(totalBytesToWrite, out.getPos());

		out.close();
		
		// Validate the expectations.
		Assert.assertEquals(storedSubBlockesExpectation, storeMock.storeSubBlockCount);
		
		// This is always one.
		Assert.assertEquals(1, storeMock.storeINodeCount);
		
		int totalBlocks = calculateTotalBlocks(totalBytesToWrite, blockSize);
		
		// Assert the total blocks per file
		Assert.assertEquals(totalBlocks, storeMock.inodesStored.get(0).getBlocks().length);
	}
	
	/** Mock class for CassandraFileSystemStore that performs no operations against the DB. 
	 *  It can be replaced for EasyMock.
	 *  Not all methods are used.
	 * @author patricioe (Patricio Echague - patricio@datastax.com)
	 */
	private class StoreMock implements CassandraFileSystemStore {

		public int storeSubBlockCount = 0;
		public int storeINodeCount = 0;
		public List<SubBlock> subBlocksStored = new ArrayList<SubBlock>();
		public List<INode> inodesStored = new ArrayList<INode>();

		@Override
		public void initialize(URI uri, Configuration conf) throws IOException {}

		@Override
		public String getVersion() throws IOException {
			return "Dummy Cassandra FileSystem Thrift Store";
		}

		@Override
		public void storeINode(Path path, INode inode) throws IOException {
			storeINodeCount++;
			inodesStored.add(inode);
		}

		@Override
		public void storeSubBlock(SubBlock block, ByteArrayOutputStream file) throws IOException {
			storeSubBlockCount++;
			subBlocksStored.add(block);
		}

		@Override
		public INode retrieveINode(Path path) throws IOException {
			return null;
		}

		@Override
		public InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException {
			return null;
		}
			
		@Override
		public void deleteINode(Path path) throws IOException {}

		@Override
		public void deleteBlock(Block block) throws IOException {}

		@Override
		public Set<Path> listSubPaths(Path path) throws IOException {
			return null;
		}

		@Override
		public Set<Path> listDeepSubPaths(Path path) throws IOException {
			return null;
		}

		@Override
		public BlockLocation[] getBlockLocation(List<Block> usedBlocks, long start, long len) throws IOException {
			return null;
		}

		@Override
		public InputStream retrieveSubBlock(SubBlock subBlock,
				long byteRangeStart) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

	}
}
