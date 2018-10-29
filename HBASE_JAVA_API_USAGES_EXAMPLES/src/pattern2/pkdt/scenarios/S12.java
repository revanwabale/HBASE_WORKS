package pattern2.pkdt.scenarios;

/*
 *  * Compile and run with:
 *   * javac -cp `hbase classpath` Connect3.java 
 *    * java -cp `hbase classpath` Connect3
 *     */
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
public class S12 {

public static void main(String[] args) throws ServiceException,IOException {


	List<String> dt_list = new ArrayList<String>();
	dt_list.add("201607");
	dt_list.add("20170701");
	
	try {

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.company.biz");
    configuration.set("zookeeper.znode.parent", "/hbase-secure");
    configuration.set("hbase.master.port", "16000");
    configuration.set("hbase.master", "sj016570.qa.bdp.company.biz");
    TableName tableName = TableName.valueOf("ASP20514:pkdt");
    Connection conn = ConnectionFactory.createConnection(configuration);
    Table tab1 =  conn.getTable(tableName);
       

	String file="../../../Scenario1_Pack100";
	List<String> rowkey_list = new ArrayList<String>();
	int cnt = 1;
	long start_time = System.currentTimeMillis();

	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	    String line;

        //long start_time = System.currentTimeMillis();
        //System.out.println("StartTime of Program(millis): "+start_time); */
	    while ((line = br.readLine()) != null) {

	    	line = line.substring(1, line.length()-1);
	    	String start = line.concat(dt_list.get(0));
	    	String stop = line.concat(dt_list.get(1));
	    	
	    	System.out.println("start row: = "+start+" Stop row:="+stop+" query cnt:="+cnt);	
	    	
	    	Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
            	int numberOfRows = 50883020;
	    	scan.setCaching(numberOfRows);
	    	scan.setCacheBlocks(true);
		scan.setBatch(numberOfRows);
		/*long st_time = System.currentTimeMillis();
		System.out.println("Query StartTime(miliis): "+st_time+" QueryNumber: "+cnt);  */
		ResultScanner scanner = tab1.getScanner(scan);
	    
		for (Result result = scanner.next(); result != null; result = scanner.next())
	    	{

    		//PrintStream original = System.out;
   		//System.setOut(new PrintStream(new FileOutputStream("/dev/null")));

		System.out.println("records: "+Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("r"))));

		 //System.setOut(original);
	    	}
      	/*	long et_time = System.currentTimeMillis();
		System.out.println("Query FinishTime(millis): "+et_time+" Time taken by this Query(millis): "+(et_time - st_time)+"; QueryNumber: "+cnt);
*/
	    	cnt ++;
	    }

// long finish_time = System.currentTimeMillis();
// System.out.println("\n**** FinishTime of Program(millis):"+finish_time+" Time taken by Program(milisec):-"+(finish_time - start_time)+" QueryNumber: "+(cnt-1));
	}
	
long finish_time = System.currentTimeMillis();
	 System.out.println("\n**** FinishTime of Program(millis):"+finish_time+" Time taken by Program(milisec):-"+(finish_time - start_time)+" QueryNumber: "+(cnt-1));

      	} finally {

		}

       
	   }

	}
