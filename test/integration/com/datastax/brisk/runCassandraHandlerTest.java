package com.datastax.brisk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DriverManager;
import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class runCassandraHandlerTest {
	public static Connection connection = null;
	    
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//Test Database Connection
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
	    connection = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    	
	    //Generate Demo Data
	    String rootDir = System.getProperty("user.dir");
	    String demoDir = rootDir + "/demos/portfolio_manager";
        
	    String[] commands = {"cd " + demoDir + ";ant",
	    					 "cd " + demoDir + ";./bin/pricer -o INSERT_PRICES",
	    					 "cd " + demoDir + ";./bin/pricer -o UPDATE_PORTFOLIOS",
	    					 "cd " + demoDir + ";./bin/pricer -o INSERT_HISTORICAL_PRICES -n 100",
	    					 rootDir + "/bin/brisk hive -f " + demoDir + "/10_day_loss.q"
	    					 };
	    
	    for(int i=0; i<commands.length ;i++){

	    	try {
	    		System.out.println("Setting up demo: " + commands[i]);
	    		Process proc = Runtime.getRuntime().exec(commands[i]);
	    		BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	    		String commandResult = null;     
	    		
	    		while((commandResult = br.readLine()) != null) {
	    			System.out.println(commandResult);
	    		    System.out.flush();
	    		}            
	    } catch (IOException e) {
	          System.exit(0);
		      }
	    }
	}
			
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		connection.close();
	}
		
	@Test
	public void testCreateLoadDropTable() throws Exception {
		System.out.println("===> cassCRUDtable: Create External C* Table, Load Data and Drop");	
		HiveTestRunner.runQueries(connection, "cassCRUDtable"); 
	} 

}
