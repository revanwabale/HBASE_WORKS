package blms.hbase.test.inprogress;

/*
 * Compile and run with:
 * javac -cp `hbase classpath` StartRowStopRow_search2.java 
 *  
 * java -cp `hbase classpath` StartRowStopRow_search2 > log
 */

import java.io.IOException;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
public class StartRowStopRow_search2 {

	private final static byte[] cf_idx_l = Bytes.toBytes("l");
	private final static byte[] cf_idx_d = Bytes.toBytes("d");
	private final static byte[] cf_idx = Bytes.toBytes("idx");
	private final static String start = "2011-01-01 00:00:00.000";
	private final static String stop = "2018-05-31 23:59:59.000";
	private final static Logger LOGGER = Logger.getLogger(StartRowStopRow_search2.class.getName());	

	public static void main(String[] args) throws ServiceException,IOException {


		try {		
			// This block configure the logger with handler and formatter  
			FileHandler fh = new FileHandler("StartRowStopRow_search2.log");  
			LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter); 
			LOGGER.setUseParentHandlers(false);
			// the following statement is used to log any messages  
			LOGGER.info("Log file initiated...");  
			//long st_time = System.currentTimeMillis();
			LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());  

			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.nissan.biz");
			configuration.set("zookeeper.znode.parent", "/hbase-secure");
			configuration.set("hbase.master.port", "16000");
			configuration.set("hbase.master", "sj016570.qa.bdp.nissan.biz");
			configuration.set("hbase.rpc.timeout", "36000000");
			configuration.set("hbase.client.scanner.timeout.period", "36000000");
			configuration.set("hbase.cells.scanned.per.heartbeat.check", "50883020");
			configuration.set("hbase.client.operation.timeout", "36000000");

			TableName tableName = TableName.valueOf("ASP20571:blms_probe_search2");
			Connection conn = ConnectionFactory.createConnection(configuration);
			Table tab1 =  conn.getTable(tableName);

			LOGGER.info("start row: = "+start+" Stop row:="+stop);	

			Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
			int numberOfRows = Integer.MAX_VALUE;
			scan.setCaching(numberOfRows);
			scan.setCacheBlocks(true);
			scan.setBatch(numberOfRows);
			/*long st_time = System.currentTimeMillis();
			System.out.println("Query StartTime(miliis): "+st_time+" QueryNumber: "+cnt);  */
			ResultScanner scanner = tab1.getScanner(scan);
			
			Result[] result = scanner.next(numberOfRows);
			/*for (Result result = scanner.next(numberOfRows); result != null; result = scanner.next())
			{

				//PrintStream original = System.out;
				//System.setOut(new PrintStream(new FileOutputStream("/dev/null")));
				System.out.println("records: "+Bytes.toString(result.getValue(cf_idx,cf_idx_l)) + Bytes.toString(result.getValue(cf_idx,cf_idx_d)));

				//System.setOut(original);
			}*/
			LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());

		}finally{
			  
			LOGGER.info("In Finally,  EndTime(miliis): "+System.currentTimeMillis());	
		}

	
}

/*
 *  authException in thread "main" java.lang.OutOfMemoryError: Requested array size exceeds VM limit
        at java.util.ArrayList.<init>(ArrayList.java:146)
        at org.apache.hadoop.hbase.client.AbstractClientScanner.next(AbstractClientScanner.java:68)
        at StartRowStopRow_search2.main(StartRowStopRow_search2.java:82)
        
 */

}
