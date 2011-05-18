package com.datastax.brisk;

import java.sql.DriverManager;
import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class runHiveExamplesTest {
    public static Connection connection = null;
    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        connection = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");       
    }
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
	
    //@Ignore
   @Test
    public void movieline_u_data() throws Exception {
		System.out.println("====> movieline_u_data: load and query u_data from movie line demo");	
		HiveTestRunner.runQueries(connection, "movieline_u_data"); 
    }   
   
   @Ignore
   @Test
    public void apache_weblog() throws Exception {
		System.out.println("====> apache_weblog: Run Apache Weblog");	
		HiveTestRunner.runQueries(connection, "apache_weblog"); 
    } 

}
