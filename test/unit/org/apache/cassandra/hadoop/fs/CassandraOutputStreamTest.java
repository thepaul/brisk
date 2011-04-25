/**
 * 
 */
package org.apache.cassandra.hadoop.fs;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.EmbeddedServer;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author patricioe (Patricio Echague - patricio@datastax.com)
 *
 */
public class CassandraOutputStreamTest extends CleanupHelper
{
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
	 * Writes several bytes until generating an overflow so that we can test the end and beginning of the new block.
	 * BlockSize used 1024 bytes. (1K)
	 * 
	 */
	@Test
	public void testWriteAndGetPosition() throws Exception {
		int blockSize = 1024;
		int bufferSize = 512;
		
		// Null object here are not needed or irrelevant for this test case. 
		// buffer size different from bytes to write is intentional.
		StoreMock storeMock = new StoreMock();
		out = new CassandraOutputStream(null, storeMock, null, null, blockSize, null, bufferSize);
		
		Assert.assertEquals(0, out.getPos());
		
		for (int i = 0; i < blockSize; i++) {
			out.write(i);
		}
		
		Assert.assertEquals(blockSize, out.getPos());
		
		// Internally the OutputStream should swith blocks during the next write.
		// Write 50 more bytes.
		for (int i = 0; i < 50; i++) {
			out.write(i);
		}
		
		// Verify that position is that beginning again.
		Assert.assertEquals(1074, out.getPos());
		
		// Validate the expectations.
		Assert.assertEquals(1, storeMock.storeBlockCount);
		Assert.assertEquals(1, storeMock.storeINodeCount);
	}
	
	/**
	 * Writes several bytes until generating an overflow so that we can test the end and beginning of the new block.
	 * BlockSize used 1024 bytes. (1K)
	 * 
	 */
	@Test
	public void testWriteArrayAndGetPosition() throws Exception {
		int blockSize = 1024;
		int bufferSize = 512;
		
		// Null object here are not needed or irrelevant for this test case. 
		// buffer size different from bytes to write is intentional.
		StoreMock storeMock = new StoreMock();
		out = new CassandraOutputStream(null, storeMock, null, null, blockSize, null, bufferSize);
		
		Assert.assertEquals(0, out.getPos());
		
		// Fill up the buffer
		byte[] buffer = new byte[blockSize];
		for (int i = 0; i < blockSize; i++) {
			buffer[i] = (byte) i;
		}
		
		// Invoke the method being tested.
		out.write(buffer, 0, blockSize);
		
		Assert.assertEquals(blockSize, out.getPos());
		
		// Internally the OutputStream should swith blocks during the next write.
		// Write 50 more bytes.
		for (int i = 0; i < 50; i++) {
			buffer[i] = (byte) i;
		}
		
		// Invoke the method being tested.
		out.write(buffer, 0, 50);
		
		// Verify that position is that beginning again.
		Assert.assertEquals(1074, out.getPos());
		
		// Validate the expectations.
		Assert.assertEquals(1, storeMock.storeBlockCount);
		Assert.assertEquals(1, storeMock.storeINodeCount);
	}
	
	/** Mock class for CassandraFileSystemStore that performs no operations against the DB. 
	 *  I can be replaced for EasyMock.
	 * @author patricioe (Patricio Echague - patricio@datastax.com)
	 *
	 */
	private class StoreMock implements CassandraFileSystemStore {

		public int storeBlockCount = 0;
		public int storeINodeCount = 0;

		@Override
		public void initialize(URI uri, Configuration conf) throws IOException {}

		@Override
		public String getVersion() throws IOException {
			return "Dummy Cassandra FileSystem Thrift Store";
		}

		@Override
		public void storeINode(Path path, INode inode) throws IOException {
			storeINodeCount++;
		}

		@Override
		public void storeBlock(Block block, ByteArrayOutputStream file) throws IOException {
			storeBlockCount++;
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

	}
}
