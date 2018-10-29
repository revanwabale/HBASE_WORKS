package pattern2.pkdt.scenarios;

/*
 * Compile and run with:
 * javac -cp `hbase classpath` Connect3.java 
 * java -cp `hbase classpath` Connect3
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
public class S81_VIN_TBL {
	private final static Logger LOGGER = Logger.getLogger(S81_VIN_TBL.class.getName());	
	private final static byte[] f = Bytes.toBytes("f");
	private final static byte[] pd =Bytes.toBytes("pd");
	private final static byte[] r =Bytes.toBytes("r");
	private final static byte[] v =Bytes.toBytes("v");
	private final static   String str_date ="20160701";
	private final static    String end_date ="20170630";

	public static void main(String[] args) throws ServiceException,IOException {

    try {  

        // This block configure the logger with handler and formatter  
        FileHandler fh = new FileHandler("S81_VIN_TBL_usingDateReange.log");  
        LOGGER.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter); 
        LOGGER.setUseParentHandlers(false);

        // the following statement is used to log any messages  
        LOGGER.info("Log file initiated...");  

    } catch (SecurityException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  

	//Define the date range for start and end rowkey scan 
	//List<String> dt_list = new ArrayList<String>();
	//dt_list.add("20170601");
	//dt_list.add("20170701");
	
	try {

	Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
    configuration.set("zookeeper.znode.parent", "/hbase-secure");
    configuration.set("hbase.master.port", "16000");
    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
    configuration.set("hbase.rpc.timeout", "36000000");
    configuration.set("hbase.client.scanner.timeout.period", "36000000");
    configuration.set("hbase.cells.scanned.per.heartbeat.check", "50883020");
    configuration.set("hbase.client.operation.timeout", "36000000");
    
    TableName vinTbl = TableName.valueOf("ASP20514:vin");
    Connection conn = ConnectionFactory.createConnection(configuration);
    Table vin_tbl =  conn.getTable(vinTbl);

    //System.out.println("StartTime:"+st_time);
	//Read the Scenario file
	//Scenario 1 condition 3: 1000 pack for 12 months data.
    //    "../../../Scenario1_Pack1000.txt
	String file="../../../Scenario8_vin1";
	LOGGER.info("Input file: "+file);
	//String file="../../../Scenario8_vin10";
	//String file="../../../Scenario8_vin100";



	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;

	    while ((line = br.readLine()) != null) {
	    	 Scan scan = new Scan(); 
	    	 scan.setStartRow(Bytes.toBytes(line.concat(str_date)));
	    	 scan.setStopRow(Bytes.toBytes(line.concat(end_date)));
	    	
		    	int numberOfRows = 50883020;
		    	scan.setCaching(numberOfRows);
		    	scan.setCacheBlocks(true);
		    	scan.setBatch(numberOfRows);
		    	scan.setMaxResultSize(numberOfRows);
		      	
		    	ResultScanner scanner = vin_tbl.getScanner(scan);
		    			    	
		    	ArrayList<String> pkdt_keys = new ArrayList<String>();
		    	//ArrayList<Bytes> pkdt_bytekeys = new ArrayList<Bytes>();
			     long st_time = System.currentTimeMillis();
			     LOGGER.info("st_time: "+st_time);
			     
		    	for (Result result = scanner.next(); result != null; result = scanner.next())
		    	{  // Bytes.toString(result.getValue(f,pd));
		    		byte[] value_byte_arr =result.getValue(f,pd);
		    		//pkdt_bytekeys.add(result.getValue(f,pd));
			         String keys_string = Bytes.toString(value_byte_arr);
			     	 String[] keys = keys_string.substring(1, keys_string.length()-1).split(",");
			     	 for (String str : keys) 
			     		 pkdt_keys.add(str);
		    		 //pkdt_keys.add(Bytes.toString(value_byte_arr));
		    	}
		    	
		   
			/*	for (String str: pkdt_keys) {
		    		System.out.println("Record: "+str);
		    	}  */
		    	LOGGER.info("scan.toString() : "+scan.toString());
		    	scanner.close();
		        vin_tbl.close();
		        
		       //######Execute the GET request using fetched keys.
		        
		        LOGGER.info("Number of keys available from vin tbl: "+ pkdt_keys.size());
		     	  TableName pkdtTbl = TableName.valueOf("ASP20514:pkdt");
		     	  Table pkdt_tbl =  conn.getTable(pkdtTbl);
		     	  List<Get> gets = new ArrayList<Get>();
		     	  for(String key: pkdt_keys) {
		     	        gets.add(new Get(Bytes.toBytes(key)));
		     	        //System.out.println(key);	
	  					}
		     	  
		     	  Result[] result_pkdt = pkdt_tbl.get(gets);
		         
		         for (Result res : result_pkdt) {
		      	   System.out.println("record: "+Bytes.toString(res.getValue(f,r))+ "VIN:"+Bytes.toString(res.getValue(f,v)));
		         	}
		         long et_time = System.currentTimeMillis();
		         LOGGER.info("et_time: "+et_time + "; st_time: "+st_time+"; TimetakenBy Query: "+(et_time - st_time));
		         
		         pkdt_tbl.close();
		         conn.close();
		        
		        
		    
	    	 /*##########################################################################
	    	 * Using Get and just "vin" as key for table to lookup the pkdt nd dtpk keys.
	    	 * #######  
	    	 
	    	 Get get = new Get(Bytes.toBytes(line));
	    	 Result result = vin_tbl.get(get);
	         vin_tbl.close();
	     	   //System.out.println("record: "+Bytes.toString(result.getValue(f,dp)));
	         String keys_string = Bytes.toString(result.getValue(f,pd));
	     	 String[] keys = keys_string.substring(1, keys_string.length()-1).split(",");
	    
	     	  for(String key: keys) {
	     	        gets.add(new Get(Bytes.toBytes(key)));
	     	        //System.out.println(key);	
  					}
	     	  LOGGER.info("Number of keys for thsi vin: "+ keys.length);
	   	
	     	  TableName pkdtTbl = TableName.valueOf("ASP20514:pkdt");
	     	  Table pkdt_tbl =  conn.getTable(pkdtTbl);
	     	  Result[] result_pkdt = pkdt_tbl.get(gets);
	         
	         for (Result res : result_pkdt) {
	      	   System.out.println("record: "+Bytes.toString(res.getValue(f,r))+ "VIN:"+Bytes.toString(res.getValue(f,v)));
	         }
	         long et_time = System.currentTimeMillis();
	         LOGGER.info("et_time: "+et_time + "; st_time: "+st_time+"; TimetakenBy Query: "+(et_time - st_time));
	         
	         pkdt_tbl.close();
	         conn.close();
	     	 ####################################*/   
	    	 
	    	 
	    	 
	    	 
	    	 
	    	 
	     	  }
			}

	}finally{}

	}
	
}	
