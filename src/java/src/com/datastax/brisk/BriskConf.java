package com.datastax.brisk;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads brisk configuration file specified by the system prop "brisk.conf".
 * 
 * @author patricioe
 *
 */
public class BriskConf {

    private static final Logger logger = LoggerFactory.getLogger(BriskConf.class);
    
    private static final String BRISK_CONF_DEFAULT_LOCATION = "resources/brisk/conf/brisk.cfg";

    private static Properties properties;

    public synchronized static void initialize() {
        if (properties != null)
        {
            return;
        }
        
        properties = new Properties();
        
        String briskConfFileLocation = System.getProperty("brisk.conf");
        
        if (briskConfFileLocation == null) 
        {
            briskConfFileLocation = BRISK_CONF_DEFAULT_LOCATION;
        }

        logger.info("Loading brisk config file from:" + briskConfFileLocation);

        loadProperties(briskConfFileLocation);
    }

    /**
     * Loads the configuration/properties file.
     * 
     * @param briskConfFileLocation brisk properties file location
     */
    private static void loadProperties(String briskConfFileLocation) {
        InputStream in = null;
        try {
            in = new FileInputStream(briskConfFileLocation);
            properties.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Error loading Brisk properties file", e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error loading Brisk properties file", e);
        } finally {
            closeQuietly(in);
        }
    }

    /**
     * Close quietly an InputStream. Logs the exception if any and continue.
     * @param in input stream
     */
    private static void closeQuietly(InputStream in) {
        if (in == null ) {
            // Nothing to close.
            return;
        }
        try {
            in.close();
        } catch (IOException e) {
            logger.warn("Closing input stream generated an error. Ingoniring it.", e);
        }
    }

    /**
     * Retrieves the Consistency Level for read operations.
     * @return ConsistencyLevel for reads
     */
    public static ConsistencyLevel getConsistencyLevelRead() {
        return ConsistencyLevel.valueOf(properties.getProperty("brisk.consistencylevel.read", "QUORUM"));
    }
    
    /**
     * Retrieves the Consistency Level for write operations.
     * @return ConsistencyLevel for write
     */
    public static ConsistencyLevel getConsistencyLevelWrite() {
        return ConsistencyLevel.valueOf(properties.getProperty("brisk.consistencylevel.write", "QUORUM"));
    }
}
