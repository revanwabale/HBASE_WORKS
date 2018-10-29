/*
 * javac -classpath `hadoop classpath`:`hbase classpath` blms/hbase/test/MultiScanTest.java
 */
package blms.hbase.test.inprogress;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MultiScanTest {

	public static void main(String[] args) throws IOException  {
	    try {  
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
	    		Connection conn = ConnectionFactory.createConnection(configuration);
	    		TableName blms_pack_info_tbl = TableName.valueOf("ASP20571:blms_pack_info_multi_scan");
	    		Table blms_pack_info_tblConn =  conn.getTable(blms_pack_info_tbl);
	    		Scan scan = new Scan(); 

	    		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	    		allFilters.addFilter(new PrefixFilter(Bytes.toBytes("pack1")));
	    		allFilters.addFilter(new PrefixFilter(Bytes.toBytes("pack2")));
	    		allFilters.addFilter(new PrefixFilter(Bytes.toBytes("pack3")));
	    		
	    		scan.setFilter(allFilters);
	    		scan.setReversed(true); 
	    		//scan.setMaxResultSize(1);
	    		System.out.println("scan object to string before execute: "+scan.toJSON());
	    	    ResultScanner scanner = blms_pack_info_tblConn.getScanner(scan);

	    		blms_pack_info_tblConn.close();
	    		 for (Result result = scanner.next(); result != null; result = scanner.next())
	   	 		 	{
	    			 System.out.println("value: "+Bytes.toString(result.getValue(Bytes.toBytes("pk"), Bytes.toBytes("ts"))));
	   	 		 	}
	    		 
	  		   	scanner.close(); 
			
	    	}finally {
				
			}
	    }
}