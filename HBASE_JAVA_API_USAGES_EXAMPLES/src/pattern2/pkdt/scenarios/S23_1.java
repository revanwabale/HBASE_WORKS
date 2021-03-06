/*
 * Compile and run with:
 * javac -cp `hbase classpath` Connect3.java 
 * java -cp `hbase classpath` Connect3
 */

package pattern2.pkdt.scenarios;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

public class S23_1 implements Runnable {
	Scan scan =null;
	//private static BufferedWriter br;
	private final static Logger LOGGER = Logger.getLogger(S23_1.class.getName());	
	//Table tab1 =null;
	public S23_1(Scan scan) throws IOException {
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
		
	    try {  

	        // This block configure the logger with handler and formatter  
	        FileHandler fh = new FileHandler("S23_1.log");  
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

Configuration configuration = HBaseConfiguration.create();
configuration.set("hbase.zookeeper.property.clientPort", "2181");
configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
configuration.set("zookeeper.znode.parent", "/hbase-secure");
configuration.set("hbase.master.port", "16000");
configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
TableName tableName = TableName.valueOf("ASP20514:pkdt");
Connection conn = ConnectionFactory.createConnection(configuration);
Table tab1 =  conn.getTable(tableName);
	   
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
	//long st_time = System.currentTimeMillis();
    //br.write("StartTime of Program: "+st_time);
    
	   long st_time = System.currentTimeMillis();
	   LOGGER.info("StartTime:"+st_time);
    int cnt = 1;
    for (Date dt : dates) {
    	
    	/*FuzzyRowFilter rowFilter = new FuzzyRowFilter(
   			 Arrays.asList(
   			  new Pair<byte[], byte[]>(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
   			  		+ "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"+dt+"\\x00\\x00"),
   					  new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1})));  */
    	
    	FuzzyRowFilter rowFilter = new FuzzyRowFilter(
      			 Arrays.asList(
      			  new Pair<byte[], byte[]>(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
      			  		+ "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"+formatter.format(dt)),
      					  new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0})));
   	
		Scan scan = new Scan(); 
		scan.setFilter(rowFilter);
    	int numberOfRows = 50883020;
    	scan.setCaching(numberOfRows);
    	scan.setCacheBlocks(true);
    	scan.setBatch(numberOfRows);
       	
    	LOGGER.info("Executiong rowfilter: "+rowFilter);
    	LOGGER.info("Executiong Query: "+cnt);
        ResultScanner scanner = tab1.getScanner(scan);
     
    	for (Result result = scanner.next(); result != null; result = scanner.next())  		
		  	    System.out.println("date: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("d")))+" Records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));
       
    	cnt ++;
    }      

   long et_time = System.currentTimeMillis();
// System.out.println("StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));
  LOGGER.info("CNT: "+ (cnt-1) +" ;StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));
  tab1.close();
  conn.close(); 

  }finally {
	

   } 

}


     

@Override
public void run() {
	// TODO Auto-generated method stub


       }
		
	}