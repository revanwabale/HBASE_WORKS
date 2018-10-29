package blms.hbase.test.inprogress;

/*
 * Compile and run with:
 * javac -cp `hbase classpath` test.java 
 *  
 * java -cp `hbase classpath` test > log
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
public class test {
	private final static byte[] cf_trip = Bytes.toBytes("t");
	private final static String probe_key = "dummyVin20180516010101";
	private final static String trip_event_id = "TRIP";
	private final static byte[] cf_trip_n = Bytes.toBytes("n");
	private final static byte[] cf_cmn_d = Bytes.toBytes("d");
	private final static byte[] cf_cmn = Bytes.toBytes("cmn");

public static void main(String[] args) throws ServiceException,IOException {

  	
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
    
    
    TableName tableName = TableName.valueOf("ASP20571:blms_probe");
    Connection conn = ConnectionFactory.createConnection(configuration);
    Table tab1 =  conn.getTable(tableName);

    Get get = new Get(Bytes.toBytes(probe_key));

	FilterList event_Filters = new FilterList(FilterList.Operator.MUST_PASS_ONE); 
	
	event_Filters.addFilter(new SingleColumnValueFilter(cf_trip, cf_trip_n, CompareOp.EQUAL, new SubstringComparator(trip_event_id)));

	get.setFilter(event_Filters);
	
	get.addColumn(cf_trip, cf_trip_n);
	
    get.isCheckExistenceOnly();
	
    System.out.println("get signature: " + get.toString());
	boolean exist = tab1.exists(get);

	//if(exist)
	if(exist)
	{
	System.out.println("exist boolean Value: "+exist);
	Result result = tab1.get(get);
	System.out.println("trip_value for event id: " + Bytes.toString(result.getValue(cf_trip, cf_trip_n)));
	System.out.println("cmn d col: " + Bytes.toString(result.getValue(cf_cmn, cf_cmn_d)));
	}else
		//System.out.println("Not exist boolean Value: "+exist);
		System.out.println("Not exist boolean Value: "+exist);
	
	    }finally{}
	
		}

	}
	
	

