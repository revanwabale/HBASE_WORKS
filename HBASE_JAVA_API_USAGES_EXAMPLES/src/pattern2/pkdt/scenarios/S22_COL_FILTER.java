/*
 * Compile and run with:
 * javac -cp `hbase classpath` Connect3.java 
 * java -cp `hbase classpath` Connect3
 */

package pattern2.pkdt.scenarios;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class S22_COL_FILTER implements Runnable {
	Scan scan =null;
	//private static BufferedWriter br;
	private final static Logger LOGGER = Logger.getLogger(S22_COL_FILTER.class.getName());
	
	//Table tab1 =null;
	public S22_COL_FILTER(Scan scan) throws IOException {
		   this.scan = scan;
			}
	
	
/*public class GetDates {	
public List getDateList(String str_date, String end_date) throws ParseException {

	List<Date> dates = new ArrayList<Date>();

	//String str_date ="20160701";  
	//String end_date ="20170630";

	DateFormat formatter ; 

	formatter = new SimpleDateFormat("yyyyMMdd");
	//formatter = new SimpleDateFormat("dd/MM/yyyy");
	Date  startDate = (Date)formatter.parse(str_date); 
	Date  endDate = (Date)formatter.parse(end_date);
	long interval = 24*1000 * 60 * 60; // 1 hour in millis
	long endTime =endDate.getTime() ; // create your endtime here, possibly using Calendar or Date
	long curTime = startDate.getTime();
	while (curTime <= endTime) {
	    dates.add(new Date(curTime));
	    curTime += interval;
	}
	for(int i=0;i<dates.size();i++){
	    Date lDate =(Date)dates.get(i);
	    String ds = formatter.format(lDate);    
	    System.out.println(" Date is ..." + ds);
	}
	return dates;
}
	}
	
	*/
	
public static void main(String[] args) throws ServiceException,IOException, InterruptedException, ParseException {
	
    try {  

        // This block configure the logger with handler and formatter  
        FileHandler fh = new FileHandler("S22_COL_FILTER.log");  
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
	
	//Logger.getLogger("org.apache.zookeeper").setLevel(LogLevel.WARN);
	//Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(LogLevel.WARN);
	//Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.WARN);
	//final Logger LOG = (Logger) LogFactory.getLog(S13.class);
	//Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
	//Logger log = Logger.getRootLogger();
	//log.info("Log initiated");
	List<String> thread_list = new ArrayList<String>();
    List<String> dt_list = new ArrayList<String>();
	dt_list.add("201607");
	dt_list.add("201608");
	dt_list.add("201609");
	dt_list.add("201610");
	dt_list.add("201611");
	dt_list.add("201612");
	dt_list.add("201701");
	dt_list.add("201702");
	dt_list.add("201703");
	dt_list.add("201704");
	dt_list.add("201705");
	dt_list.add("201706");

	//pass the datelist for fuzzy logic
	/*
	String str_date ="20160701";  
	String end_date ="20170630";

	List<Date> dates = new ArrayList<Date>();

	//String str_date ="20160701";  
	//String end_date ="20170630";

	DateFormat formatter ; 

	formatter = new SimpleDateFormat("yyyyMMdd");
	//formatter = new SimpleDateFormat("dd/MM/yyyy");
	Date  startDate = (Date)formatter.parse(str_date); 
	Date  endDate = (Date)formatter.parse(end_date);
	long interval = 24*1000 * 60 * 60; // 1 hour in millis
	long endTime =endDate.getTime() ; // create your endtime here, possibly using Calendar or Date
	long curTime = startDate.getTime();
	while (curTime <= endTime) {
	    dates.add(new Date(curTime));
	    curTime += interval;
	}
/* for(int i=0;i<dates.size();i++){
	    Date lDate =(Date)dates.get(i);
	    String ds = formatter.format(lDate);    
	    System.out.println(" Date is ..." + ds);
	}  */
	
	//String file="log_java_run.txt";
   // br = new BufferedWriter(new FileWriter(file)); 	
    //List<Scan> scans = new ArrayList<Scan>();
	

	long st_time = System.currentTimeMillis();
    //br.write("StartTime of Program: "+st_time);
	LOGGER.info("StartTime:"+st_time);
	FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    for (String dt : dt_list) {
    	
    	allFilters.addFilter(new FuzzyRowFilter(
   			 Arrays.asList(
   			  new Pair<byte[], byte[]>(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
   			  		+ "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"+dt+"\\x00\\x00"),
   					  new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1}))));  
    }
    	/*FuzzyRowFilter rowFilter = new FuzzyRowFilter(
      			 Arrays.asList(
      			  new Pair<byte[], byte[]>(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
      			  		+ "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"+formatter.format(dt)),
      					  new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0})));  
    		LOGGER.info("Executiong rowfilter: "+rowFilter);
    		*/
    		/*FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	    	allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("d"), 
	    			CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("20160701")));
	    	allFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("d"),
	    			CompareOp.LESS_OR_EQUAL, Bytes.toBytes("20170630")));
    		*/
	    	
	    	//allFilters.addFilter(new RowFilter
			Scan scan = new Scan(); 
			scan.setFilter(allFilters);
	    	int numberOfRows = 50883020;
	    	scan.setCaching(numberOfRows);
	    	scan.setCacheBlocks(true);
	    	//scan.setBatch(numberOfRows);
	    	//scan.setScanMetricsEnabled(true);  
     	  /* 	S22_COL_FILTER obj=new S22_COL_FILTER(scan);  
	    	//call to thread
	        Thread tobj =new Thread(obj);  
	        tobj.start();
	        tobj.setName(scan.toString());
	        LOGGER.info("Executiong thread: "+tobj.getName()+" ; Query: "+cnt);
	        tobj.join();
	        LOGGER.info("Above thread Joined in pool....");
	           
	        
	        thread_list.add(tobj.getName());
   	    	cnt ++;
   	    	
   	    	*/
	    	
	    	Configuration configuration = HBaseConfiguration.create();
	  	    configuration.set("hbase.zookeeper.property.clientPort", "2181");
	  	    configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
	  	    configuration.set("zookeeper.znode.parent", "/hbase-secure");
	  	    configuration.set("hbase.master.port", "16000");
	  	    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
	  	    TableName tableName = TableName.valueOf("ASP20514:pkdt");
	  	
	  		try {
	  			Connection conn = ConnectionFactory.createConnection(configuration);
	  
	  			Table tab1 = conn.getTable(tableName);
	  			LOGGER.info("Scan.String(): "+ scan.toString());
	  			ResultScanner scanner = tab1.getScanner(scan);
	  		
	  			    
	  		for (Result result = scanner.next(); result != null; result = scanner.next()) {  		
	  		  	    System.out.println("date: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("d")))+" Records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));
	  		  	    //br.write("getTotalSizeOfCells:  "+(int) Result.getTotalSizeOfCells(result));
	  		}
	  		
	  		long et_time = System.currentTimeMillis();
	  	    //System.out.println("StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));
	  	    //br.write("StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));
	  	    LOGGER.info(" StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));

	  		scanner.close();
	  		tab1.close();
	  		conn.close();
	  	    }finally {} 

  }  

@Override
public void run() {
	// TODO Auto-generated method stub
	  Configuration configuration = HBaseConfiguration.create();
	    configuration.set("hbase.zookeeper.property.clientPort", "2181");
	    configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
	    configuration.set("zookeeper.znode.parent", "/hbase-secure");
	    configuration.set("hbase.master.port", "16000");
	    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
	    TableName tableName = TableName.valueOf("ASP20514:pkdt");
	    Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(configuration);
			//br.write("Config Parameter:= "+conn.getConfiguration().getFinalParameters());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    Table tab1 = null;
		try {
			tab1 = conn.getTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    ResultScanner scanner = null;
		try {
			scanner = tab1.getScanner(scan);
		
			    
		for (Result result = scanner.next(); result != null; result = scanner.next()) {  		
		  	    System.out.println("date: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("d")))+" Records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));
		  	    //br.write("getTotalSizeOfCells:  "+(int) Result.getTotalSizeOfCells(result));
		}
	
		//ScanMetrics scan_metrics = scan.getScanMetrics();
		//br.write("scan metrics: "+ScanMetrics.REMOTE_RPC_CALLS_METRIC_NAME);
		//br.write("scan metrics: "+ScanMetrics.);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	
		
		}finally {
		try {
			tab1.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {

			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	}

       }
		
	}