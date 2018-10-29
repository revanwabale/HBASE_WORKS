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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
public class S81 {
	private final static Logger LOGGER = Logger.getLogger(S81.class.getName());	
	
public static void main(String[] args) throws ServiceException,IOException {

	
	
    try {  

        // This block configure the logger with handler and formatter  
        FileHandler fh = new FileHandler("S81.log");  
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
    
    TableName tableName = TableName.valueOf("ASP20514:pkdt");
    Connection conn = ConnectionFactory.createConnection(configuration);
    Table tab1 =  conn.getTable(tableName);

    //System.out.println("StartTime:"+st_time);
	//Read the Scenario file
	//Scenario 1 condition 3: 1000 pack for 12 months data.
    //    "../../../Scenario1_Pack1000.txt
	String file="../../../Scenario8_vin1";
	//String file="../../../Scenario8_vin10";
	//String file="../../../Scenario8_vin100";
	
	FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	int cnt = 1;
	LOGGER.info("Input file: "+file);
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;
	    while ((line = br.readLine()) != null) {
	       // process the line.
		       //List<Get> queryRowList = new ArrayList<Get>();
	           //queryRowList.add(new Get(Bytes.toBytes(line+dt_list.get(0))));
	    	//line = line.substring(1, line.length()-1);
	    	//String start = line.concat(dt_list.get(0));
	    	//String stop = line.concat(dt_list.get(1));
	    	
	        // String start = line;
	    	//String stop = line;
	    	
	    	//System.out.println("start row: = "+start+" Stop row:="+stop+"query cnt:="+cnt);	
	    	//LOGGER.info("start row: = "+start+" Stop row:="+stop+"query cnt:="+cnt);
	    	
	    	//Filter filter = new PrefixFilter(Bytes.toBytes("line"));
	    	//Filter filter = new Ror(Bytes.toBytes("line"));
	    /*	
	    	 FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	         allFilters.addFilter(new PrefixFilter(Bytes.toBytes("123")));
	         allFilters.addFilter(new PrefixFilter(Bytes.toBytes("456")));
	         allFilters.addFilter(new PrefixFilter(Bytes.toBytes("678")));
	        //scan.setFilter(allFilters);
		*/
	    	
	    	//Filter Cfilter = new SingleColumnValueFilter('f','d', = , 'binary:20170705');
	    	//Filter Cfilter = new SingleColumnValueFilter(Bytes.getBytes('f'), Bytes.toBytes('d'),CompareFilter::CompareOp.valueOf('EQUAL'),
	    		//	BinaryComparator.new(Bytes.toBytes('20170705'));
	    	Filter Cfilter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("v"), CompareOp.EQUAL, new SubstringComparator(line));
	    	allFilters.addFilter(Cfilter);
	    	cnt ++;
	    	
	    		}
			}
	    
	    	LOGGER.info("Number of Coulmn filter for comparison: "+ cnt);
	    	Scan scan = new Scan(); 
	    	//scan.setRowPrefixFilter(Bytes.toBytes(line));
	    	//scan.setStartRow(Bytes.toBytes(line));
	    	//scan.setStopRow(Bytes.toBytes(line));
	    	scan.setFilter(allFilters);
	    	
	    	int numberOfRows = 50883020;
	    	scan.setCaching(numberOfRows);
	    	scan.setCacheBlocks(true);
	    	//scan.setBatch(numberOfRows);
	    	scan.setMaxResultSize(numberOfRows);
	        
	    	long st_time = System.currentTimeMillis();
	 	    LOGGER.info("StartTime:"+st_time);
	 	   
	    	ResultScanner scanner = tab1.getScanner(scan);
	    	for (Result result = scanner.next(); result != null; result = scanner.next())
	    	{
	    		
	    		//System.out.println("this should go to stdout");
	    		//PrintStream original = System.out;
	    		//System.setOut(new PrintStream(new FileOutputStream("/dev/null")));

	    	    System.out.println("VIN: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("v"))) + "; Records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));
				//System.out.println("records: "+result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r")));
		    	//System.setOut(original);
		    	//System.out.println("this should go to stdout");
	    	   
	    	}
	    	long et_time = System.currentTimeMillis();
	    	//LOGGER.info("scan.getAttributesMap() : "+scan.getAttributesMap());
	    	LOGGER.info("scan.toString() : "+scan.toString());
	    	scanner.close();


	    	
	    	//System.out.println("pack: "+line);
	    	
		    tab1.close();
		    conn.close(); 
	        
	        //System.out.println("QueryCNT: "+cnt+"; FinishTime:"+et_time+"; StartTime: "+st_time+ "; Time taken by this Query: "+(et_time - st_time));
	        LOGGER.info("QueryCNT: "+(cnt-1)+"; FinishTime:"+et_time+"; StartTime: "+st_time+ "; Time taken by this Query: "+(et_time - st_time));
		
	    	
	    }finally{}

	}
	
	

	//read Scenarios                
	//create the row keys for get     
	//Preapre list to pass to get object 
	//Prepare container of multiaction
	//Get the time before execute 
	//Scrap all the output at stdout
	//get the last timestamp 
	//get the time taken
	//get the session id of execution.


	 /*    configuration.set("hbase.master.port", "16000");
	       configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
	       configuration.addResource("/usr/hdp/current/hbase-client/conf/core-site.xml");
	       configuration.addResource("/usr/hdp/current/hbase-client/conf/hbase-site.xml");
	       configuration.addResource("/usr/hdp/current/hbase-client/conf/hdfs-site.xml");


	       List<Get> queryRowList = new ArrayList<Get>();
           queryRowList.add(new Get(Bytes.toBytes("AAAA20170101")));
           queryRowList.add(new Get(Bytes.toBytes("AAAA20170102")));
           TableName tableName = TableName.valueOf("ASP20514:pkdt");
           Connection conn = ConnectionFactory.createConnection(configuration);

	       Table tab1 =  conn.getTable(tableName);
	       Result[] results = tab1.get(queryRowList);
		   Get g = new Get(Bytes.toBytes("AAAA20170101"));
	       Result result= tab1.get(g);
	       byte [] value =result.getValue(Bytes.toBytes("f"),Bytes.toBytes("p"));
	       String pack_value = Bytes.toString(value);
	 
	       for (Result r : results) {
	    	   byte [] value =r.getValue(Bytes.toBytes("f"),Bytes.toBytes("p"));
	    	   System.out.println("pack_with_space: " + value);
           }

      */ 
          
	   }

