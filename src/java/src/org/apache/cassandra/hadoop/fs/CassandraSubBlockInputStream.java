/**
 * 
 */
package org.apache.cassandra.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Inner inputStream for SubBlocks that provides an abstraction to
 * @link {@link CassandraInputStream} to read a flow of data.
 * 
 *  It handles the SubBlock swtich and closes the underlying inputstream.
 * 
 * @author patricioe (Patricio Echague - patricio@datastax.com)
 *
 */
public class CassandraSubBlockInputStream extends InputStream {
	
    private boolean                  closed;
    
    private long                     pos      = 0;

    private InputStream              subBlockStream;
    
    private Block                    block;
    
    private long                     subBlockEnd = -1;

	private long byteRangeStart;

	private CassandraFileSystemThriftStore store;

	public CassandraSubBlockInputStream(CassandraFileSystemThriftStore store, Block block, long byteRangeStart) {
		this.store = store;
		this.block = block;
		this.byteRangeStart = byteRangeStart;
		pos = byteRangeStart;
	}

	/* (non-Javadoc)
	 * @see java.io.InputStream#read()
	 */
	@Override
	public synchronized int read() throws IOException {
        if (closed)
        {
            throw new IOException("Stream closed");
        }
        int result = -1;
        if (pos < block.length)
        {
            if (pos > subBlockEnd)
            {
                subBlockSeekTo(pos);
            }
            result = subBlockStream.read();
            if (result >= 0)
            {
                pos++;
            }
        }

        return result;
	}
	
    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException
    {
        if (closed)
        {
            throw new IOException("Stream closed");
        }
        if (pos < block.length)
        {
            if (pos > subBlockEnd)
            {
            	subBlockSeekTo(pos);
            }
            int realLen = Math.min(len, (int) (subBlockEnd - pos + 1));
            int result = subBlockStream.read(buf, off, realLen);
            if (result >= 0)
            {
                pos += result;
            }
            return result;
        }
        return -1;
    }
	
    private synchronized void subBlockSeekTo(long target) throws IOException
    {
    	// Close underlying inputStream when switching to the new subBlock.
        if (this.subBlockStream != null) {
            this.subBlockStream.close();
        }

        //
        // Compute desired block
        //
        int targetSubBlock = -1;
        long targetSubBlockStart = 0;
        long targetSubBlockEnd = 0;
        
        for (int i = 0; i < block.subBlocks.length; i++)
        {
            long subBlockLength = block.subBlocks[i].length;
            targetSubBlockEnd = targetSubBlockStart + subBlockLength - 1;

            if (target >= targetSubBlockStart && target <= targetSubBlockEnd)
            {
            	targetSubBlock = i;
                break;
            }
            else
            {
                targetSubBlockStart = targetSubBlockEnd + 1;
            }
        }
        if (targetSubBlock < 0)
        {
            throw new IOException("Impossible situation: could not find target position " + target);
        }
        long offsetIntoSubBlock = target - targetSubBlockStart;

        this.pos = target;
        this.subBlockEnd = targetSubBlockEnd;
        this.subBlockStream = store.retrieveSubBlock(block, block.subBlocks[targetSubBlock], offsetIntoSubBlock);

    }
	
    @Override
    public synchronized void close() throws IOException
    {
        if (closed)
        {
            return;
        }
        
        if (this.subBlockStream != null) {
            this.subBlockStream.close();
        }
       
        super.close();
        closed = true;
    }

    /**
     * We don't support marks.
     */
    @Override
    public synchronized boolean markSupported()
    {
        return false;
    }

    @Override
    public synchronized void mark(int readLimit)
    {
        // Do nothing
    }

    @Override
    public synchronized void reset() throws IOException
    {
        throw new IOException("Mark not supported");
    }
    
    public synchronized long getPos() throws IOException
    {
        return pos;
    }

}
