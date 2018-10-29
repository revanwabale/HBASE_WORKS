/*
 * Compile and run with:
 * javac -cp `hbase classpath` S33.java 
 * java -cp `hbase classpath` S33
 */

/*
 * package name:pattern2.pkdt.scenarios
 */
package pattern2.pkdt.scenarios;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
public class S33 {

	// Instantiate Logger for this class name 
		private final static Logger LOGGER = Logger.getLogger(S33.class.getName());	
		//Alliance pack id list 
		private final static String file="../../../Scenario3_Pack1000";
	/*
	 * Main Method process:
	 * 1. Read listr of aliance pack ids from file "Scenario3_Pack1000"
	 * 2. Create the key using date range and alliance pack.
	 *     Table Name: ASP20514:pkdt
	 *     Key Format Used: AlliancePak + UpdateDate 
	 *     					ex. For AlliancePack = "295B04FA1B BA114CQ000506 "
	 *								UpdateDate = "20170601"  
	 *							Key = "295B04FA1B BA114CQ000506 20170601" 
	 * 3.craete the table connection using hbase configuration.
	 * 4.Preapre the hbase call using Scan() & Get() method
	 * 5.Read the Hbase table data.
	 * 6.Exit.    						 
	 */
	public static void main(String[] args) throws ServiceException,IOException {

		
    try {  

        // This block configure the logger with handler and formatter  
        FileHandler fh = new FileHandler("S33.log");  
        LOGGER.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter); 
        LOGGER.setUseParentHandlers(false);

        // 1st line in log file to show the loger has started and its working.
        LOGGER.info("Log file initiated...");  

    } catch (SecurityException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  

    
	/*
	 * 1. Define the date range for start and end rowkey for Scan operation
	 * 2. DateRange: 1 month=20170601 to 20170630
	 */
    List<String> dt_list = new ArrayList<String>();
	dt_list.add("20170601");
	dt_list.add("20170701");
	
	try {
		
		//Create the configuration for hbase connection using below properties.
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
	    configuration.set("zookeeper.znode.parent", "/hbase-secure");
	    configuration.set("hbase.master.port", "16000");
	    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
	    
	    //Hbase table will be used to read the data
	    TableName tableName = TableName.valueOf("ASP20514:pkdt");
	    
	    //Create the connection for above said configuration using below statement.
	    Connection conn = ConnectionFactory.createConnection(configuration);
	    Table tab1 =  conn.getTable(tableName);
	   
	    //Retain the start time of Scan process to calculate the total time taken at the end.
	    long st_time = System.currentTimeMillis();
		LOGGER.info("StartTime:"+st_time);

	/*
	 * 1. Read the Scenario file which has all listed alliance pack id for 
	 * 	  which we need to read the hbase table data over above date range for each alliance pack
	 * 	  ex. Scenario 1 condition 3: 1000 pack for 1 month data.(in POC)
	 */
	

	//Track the number of alliance packs from file 
	int cnt = 1;
	LOGGER.info("Input file: "+file);
	//Read alliance pack id from file one by one
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;
  
	    while ((line = br.readLine()) != null) {
		    //Trim for last and first character which is '"'
	    	line = line.substring(1, line.length()-1);
	    	//Create the start key and stop key for Scan operation
	    	String start = line.concat(dt_list.get(0));
	    	String stop = line.concat(dt_list.get(1));
	    	
	    	LOGGER.info("start row: = "+start+" Stop row:="+stop+"query cnt:="+cnt);
	    	
	    	//Prepare Scan using start and stop row 
	    	Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
	    	
	    	//Set properties for better hbase Scan operation 
	    	int numberOfRows = 50883020;
	    	scan.setCaching(numberOfRows);
	    	scan.setCacheBlocks(true);
	    	scan.setBatch(numberOfRows);
	    	
	    	//Execute the Scan operation for tab1 = "PKDT" table
	    	ResultScanner scanner = tab1.getScanner(scan);
	    	
	    	//Process the returned data for each Scan.
	    	for (Result result = scanner.next(); result != null; result = scanner.next())
	    	    System.out.println("records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));
  	    	
	    	//Increment the count incase if more alliance pack id present from file.
	    	cnt ++;
	    }
	    // After processing of all alliance pack, read the finish time of program to calculate total time taken by process.
        long et_time = System.currentTimeMillis();
        //Log the total time taken by process.
        LOGGER.info("QueryCNT: "+(cnt-1)+"; FinishTime:"+et_time+"; StartTime: "+st_time+ "; Time taken by this Query: "+(et_time - st_time));
	
	}
	//finally block , Nothing to process in finally block.
      } finally {

				}   
	   }

	}