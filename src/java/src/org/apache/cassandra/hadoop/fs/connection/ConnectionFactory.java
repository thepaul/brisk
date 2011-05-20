package org.apache.cassandra.hadoop.fs.connection;

import java.io.IOException;
import java.net.URI;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.CassandraProxyClient;
import org.apache.cassandra.hadoop.CassandraProxyClient.ConnectionStrategy;
import org.apache.cassandra.hadoop.trackers.CassandraJobConf;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;

import com.datastax.brisk.BriskInternalServer;

/**
 * Responsible for creating connections. 
 */
public class ConnectionFactory {
	
	private String host;
	
	private int port;
	
	private boolean isLocal = false;
	
	public ConnectionFactory(URI uri, Configuration conf) {
		
		if (conf instanceof CassandraJobConf) {
			isLocal = true;
		}
		
        host = uri.getHost();
        port = uri.getPort();

        if (host == null || host.isEmpty() || host.equals("null"))
            host = FBUtilities.getLocalAddress().getHostName();

        if (port == -1)
            port = DatabaseDescriptor.getRpcPort(); // default
	}
	
	public Cassandra.Iface createConnection() throws IOException {
		
        // We could be running inside of cassandra...
        if (isLocal)
            return new BriskInternalServer();
        else
            return CassandraProxyClient.newProxyConnection(host, port, true, ConnectionStrategy.STICKY);
	}
}
