/*
 * Compile and run with:
 * javac -cp `hbase classpath` Connect3.java 
 * java -cp `hbase classpath` Connect3
 */
package pattern2.pkdt.scenarios;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
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

public class S13 implements Runnable {
	Scan scan =null;
	//Table tab1 =null;
	public S13(Scan scan) throws IOException {
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

	//Define the date range for start and end rowkey scan 
	List<String> dt_list = new ArrayList<String>();
	dt_list.add("201607");
	dt_list.add("201706");
	
	try {

	Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
    configuration.set("zookeeper.znode.parent", "/hbase-secure");
    configuration.set("hbase.master.port", "16000");
    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
    long st_time = System.currentTimeMillis();
    System.out.println("StartTime:"+st_time);
	//Read the Scenario file
	//Scenario 1 condition 3: 1000 pack for 12 months data.
	String file="/home/asp20514bdp/revan/data/hbase_patter2_pkdt/Scenario1_Pack1000.txt";
	int cnt = 0;
	
	
	
	List<String> thread_list = new ArrayList<String>();
	
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;
	    while ((line = br.readLine()) != null) {
	       // process the line.
		       //List<Get> queryRowList = new ArrayList<Get>();
	           //queryRowList.add(new Get(Bytes.toBytes(line+dt_list.get(0))));
	    	line = line.substring(1, line.length()-1);
	    	String start = line.concat(dt_list.get(0));
	    	String stop = line.concat(dt_list.get(1));
	    	
	    	System.out.println("start row: = "+start+" Stop row:="+stop+"query cnt:="+cnt);	
	    	
	    	Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
    	
    	/*FuzzyRowFilter rowFilter = new FuzzyRowFilter(
   			 Arrays.asList(
   			  new Pair<byte[], byte[]>(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
   			  		+ "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"+dt+"\\x00\\x00"),
   					  new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1})));  
	        scan.setFilter(rowFilter);  */

	    	int numberOfRows = 50883020;
	    	scan.setCaching(numberOfRows);
	    	scan.setCacheBlocks(true);
	    	scan.setBatch(numberOfRows);
	    	//scan.setScanMetricsEnabled(true);  
     	   	S13 obj=new S13(scan);  
	    	//call to thread
	        Thread tobj =new Thread(obj);  
	        tobj.start();
	        tobj.setName(scan.toString());
	        tobj.join();
	        thread_list.add(tobj.getName());
   	    	cnt ++;
    }
	}
    
    long et_time = System.currentTimeMillis();
    System.out.println("StartTime: "+st_time+"; FinishTime(milis):"+et_time+"; Time taken by this Query(milis): "+(et_time - st_time));
 
	} finally {
	} 

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