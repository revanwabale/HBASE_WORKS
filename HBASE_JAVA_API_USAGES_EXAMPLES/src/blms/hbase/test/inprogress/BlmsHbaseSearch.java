package blms.hbase.test.inprogress;
/*
 * Compile and run with:
 * javac -cp `hbase classpath` PackinfoSearch.java 
 *  
 * nohup java -cp `hbase classpath` PackinfoSearch > /dev/null &
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
public class BlmsHbaseSearch {

	private final static byte[] cf_idx_b = Bytes.toBytes("b");
	private final static byte[] cf_idx_g = Bytes.toBytes("g");
	private final static byte[] cf_idx = Bytes.toBytes("idx");
	private final static byte[] cf_pk = Bytes.toBytes("pk");
	private final static byte[] cf_t = Bytes.toBytes("t");
	private final static byte[] cf_cmn = Bytes.toBytes("cmn");
	private final static byte[] cf_t_n = Bytes.toBytes("n"); 
	//private final static byte[] cf_cmn_k = Bytes.toBytes("k"); 
	private final static byte[] cf_cmn_g = Bytes.toBytes("g"); 
	private final static byte[] cf_cmn_a = Bytes.toBytes("a"); 
	private final static byte[] cf_cmn_k = Bytes.toBytes("k");
	private final static byte[] cf_cmn_l = Bytes.toBytes("l");
	private final static byte[] cf_cmn_b = Bytes.toBytes("b");
	private final static byte[] cf_cmn_c = Bytes.toBytes("c");
	private final static byte[] cf_cmn_d = Bytes.toBytes("d"); 

	private final static Logger LOGGER = Logger.getLogger(BlmsHbaseSearch.class.getName());	
	private static Configuration configuration;
	private static FileHandler fh;
	private static SimpleFormatter formatter;
	private static Connection conn ;
	private static String HBDB = "ASP20571";
	private static String pack_info_tab = "blms_pack_info";
	private static String blms_probe_tab = "blms_probe";
	private static String packinfo_upd_idx_tab = "blms_pack_info_upd_idx";
	//private static Table hb_tab_obj;
	//private static TableName tableName;
	private static  ResultScanner scanner ;
	private static NavigableSet<byte[]> pkInfoCols = new TreeSet<>(Bytes.BYTES_COMPARATOR);
	private static NavigableSet<String> pkInfoCols_string = new TreeSet<>();
	private static Map<byte[],NavigableSet<byte[]>> pkInfoFamilyMap = new  HashMap<byte[],NavigableSet<byte[]>>();
	//private static String file_pkinfo_col_list = "/home/asp20571bdp/REVAN/BATCH_DEV/WEBSERVICE_UT/POC_RE_TEST_PATTERN_IT/pkinfo_col_list.txt";
	private static String file_pkinfo_col_list = "D:\\Revan_WORK\\BLMS\\BLMS_PHASE-1\\INTEGRATION_TEST\\ISSUES\\WEB-SERVICE_PERFORMACE_ISSUE\\20180530_discussion_for_final_stage\\SAKAGUCHI_SAN_REQUEST_RUN_LOCAL_QUERIES_PROBE_PACKINFO\\pkinfo_col_list.txt";


	public static Table getTable(String tblName) throws IOException {
		TableName tableName = TableName.valueOf(HBDB+":"+tblName);
		Table hb_tab_obj =  conn.getTable(tableName);
		return hb_tab_obj;
	}
	public static void getResources() throws SecurityException, IOException{
		try {		
			// This block configure the logger with handler and formatter  
			fh = new FileHandler("BlmsHbaseSearch.log");  
			LOGGER.addHandler(fh);
			formatter = new SimpleFormatter();  
			fh.setFormatter(formatter); 
			LOGGER.setUseParentHandlers(false);
			// the following statement is used to log any messages  
			LOGGER.info("Log file initiated...");  
			//long st_time = System.currentTimeMillis();
			configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			configuration.set("hbase.zookeeper.quorum", "sj016570.qa.bdp.nissan.biz");
			configuration.set("zookeeper.znode.parent", "/hbase-secure");
			configuration.set("hbase.master.port", "16000");
			configuration.set("hbase.master", "sj016570.qa.bdp.nissan.biz");
			//configuration.set("hbase.rpc.timeout", "36000000");
			//configuration.set("hbase.client.scanner.timeout.period", "36000000");
			//configuration.set("hbase.cells.scanned.per.heartbeat.check", "50883020");
			//configuration.set("hbase.client.operation.timeout", "36000000");
			conn = ConnectionFactory.createConnection(configuration);

		}finally{}
	}
	
	public static Result getLatestRecord_reverseKeyOnlyFilter(String tableName, String prefixFilterValue) throws IOException {
		LOGGER.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(HBDB + ":" + tableName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		//Scan scan = new Scan(Bytes.toBytes(prefixFilterValue), Bytes.toBytes(prefixFilterValue)); 
		Scan scan = new Scan(); 
		scan.setCaching(1);//no need to bring the 100 records (default ) by each next request ; event its next() or next(nb)
		//FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL, new FirstKeyOnlyFilter(), new KeyOnlyFilter());
		filters.addFilter(new PrefixFilter(Bytes.toBytes(prefixFilterValue)));
		//scan.setRowPrefixFilter(Bytes.toBytes(prefixFilterValue));
		scan.setFilter(filters);
		scan.setReversed(true); //Read the latest available key and value
		//scan.setMaxResultSize(1);
		
		//scan.setFilter(new InclusiveStopFilter(Bytes.toBytes(prefixFilterValue)));
		
		ResultScanner scanner = blms_tblConn.getScanner(scan);
		Result result = scanner.next();
		scanner.close(); //Scanner closed
		blms_tblConn.close();
		if(result != null)
			LOGGER.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(result.getRow()));
		else
			LOGGER.log(Level.INFO, "Record not Found....");
		return result;
	}
	
	public static Result getLatestRecord(String tableName, String prefixFilterValue) throws IOException {
		LOGGER.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(HBDB + ":" + tableName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		//Scan scan = new Scan(Bytes.toBytes(prefixFilterValue), Bytes.toBytes(prefixFilterValue)); 
		Scan scan = new Scan(); 
		scan.setCaching(1);//no need to bring the 100 records (default ) by each next request ; event its next() or next(nb)
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		allFilters.addFilter(new PrefixFilter(Bytes.toBytes(prefixFilterValue)));
		//scan.setRowPrefixFilter(Bytes.toBytes(prefixFilterValue));
		scan.setFilter(allFilters);
		scan.setReversed(true); //Read the latest available key and value
		//scan.setMaxResultSize(1);
		
		//scan.setFilter(new InclusiveStopFilter(Bytes.toBytes(prefixFilterValue)));
		
		ResultScanner scanner = blms_tblConn.getScanner(scan);
		Result result = scanner.next();
		scanner.close(); //Scanner closed
		blms_tblConn.close();
		if(result != null)
			LOGGER.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(result.getRow()));
		else
			LOGGER.log(Level.INFO, "Record not Found....");
		return result;
	}
/*	
	public static Result getLatestRecordNotReversed(String tableName, String prefixFilterValue) throws IOException {
		
		//List keys = new ArrayList<Result>();
		Result LastRow=null ;
		
		LOGGER.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(HBDB + ":" + tableName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		//Scan scan = new Scan(Bytes.toBytes(prefixFilterValue), Bytes.toBytes(prefixFilterValue)); 
		Scan scan = new Scan(); 
		scan.setCaching(1000);//brings the 100 records (default ) by each next request ; even its next() or next(nb)
		//FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		//allFilters.addFilter(new PrefixFilter(Bytes.toBytes(prefixFilterValue)));
		scan.setRowPrefixFilter(Bytes.toBytes(prefixFilterValue));
		scan.getLoadColumnFamiliesOnDemandValue();
		
		//scan.setFilter(allFilters);
		//scan.setReversed(true); //Read the latest available key and value
		//scan.setMaxResultSize(1);
		//scan.setFilter(new InclusiveStopFilter(Bytes.toBytes(prefixFilterValue)));
		
		ResultScanner scanner = blms_tblConn.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			//keys.add(result);
			LastRow= result;
		}
		scanner.close(); //Scanner closed
		blms_tblConn.close();

		if(LastRow != null)
			LOGGER.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(LastRow.getRow()));
		else
			LOGGER.log(Level.INFO, "Record not Found....");

		return LastRow;
	}
	*/
	
public static Result getLatestRecordNotReversed(String tableName, String prefixFilterValue) throws IOException {
		
		//List keys = new ArrayList<Result>();
		Result LastRow=null ;
		
		LOGGER.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(HBDB + ":" + tableName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		//Scan scan = new Scan(Bytes.toBytes(prefixFilterValue), Bytes.toBytes(prefixFilterValue)); 
		Scan scan = new Scan(); 
		scan.setCaching(1000);//brings the 100 records (default ) by each next request ; even its next() or next(nb)
		//FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		//allFilters.addFilter(new PrefixFilter(Bytes.toBytes(prefixFilterValue)));
		scan.setRowPrefixFilter(Bytes.toBytes(prefixFilterValue));
		scan.getLoadColumnFamiliesOnDemandValue();
		
		//scan.setFilter(allFilters);
		//scan.setReversed(true); //Read the latest available key and value
		//scan.setMaxResultSize(1);
		//scan.setFilter(new InclusiveStopFilter(Bytes.toBytes(prefixFilterValue)));
		scan.setStartRow(org.apache.hadoop.hbase.util.Bytes.toBytesBinary(prefixFilterValue));
	//	scan.setEndRow(org.apache.hadoop.hbase.util.Bytes.toBytesBinary(prefixFilterValue.concat(String.valueOf(Long.MAX_VALUE))));
		//Long.MAX_VALUE =  9223372036854775807
		
		ResultScanner scanner = blms_tblConn.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			//keys.add(result);
			LastRow= result;
		}
		scanner.close(); //Scanner closed
		blms_tblConn.close();

		if(LastRow != null)
			LOGGER.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(LastRow.getRow()));
		else
			LOGGER.log(Level.INFO, "Record not Found....");

		return LastRow;
	}
	
	public static void deleteFromTable(String DeleteFromTable, List<Delete> DeleteRowList) {
		Table blms_tbl_outConn = null;
		try {
			TableName blms_tbl_out = TableName.valueOf(HBDB + ":" + DeleteFromTable);
			blms_tbl_outConn =  conn.getTable(blms_tbl_out);
			blms_tbl_outConn.delete(DeleteRowList);
		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER.log(Level.SEVERE, "Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				LOGGER.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
			} 
			LOGGER.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
		}catch(Exception e){
			LOGGER.log(Level.SEVERE, e.toString(), e);
		} finally {
			try {
				if (blms_tbl_outConn != null) blms_tbl_outConn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}//end deleteFromTable
	
	public static void getpkInfoFamilyMap() throws IOException {

		BufferedReader br ;
		try {

			br = new BufferedReader(new FileReader(file_pkinfo_col_list));

			String line = null;
			while ((line = br.readLine()) != null) {
				LOGGER.log(Level.INFO, "input column: " + line);

				if(line.length() !=0)
				{
					byte[] b01  = Bytes.toBytes(line.trim());
 					pkInfoCols.add(b01);
 					pkInfoCols_string.add(line);
				}

			}
			
			pkInfoFamilyMap.put(cf_pk, pkInfoCols);
			br.close();
			
			LOGGER.log(Level.INFO, "pkInfoCols: " + pkInfoCols);	
			LOGGER.log(Level.INFO, "pkInfoCols_string: " + pkInfoCols_string);
			LOGGER.log(Level.INFO, "pkInfoFamilyMap: " + pkInfoFamilyMap);	
					
		}catch(IOException e) { e.printStackTrace();

		}finally {

		}

	}

	public static void searchPackinfo_AP_UPD(String start, String stop) throws IOException{

		LOGGER.info("In searchPackinfo_AP_UPD() func");

		Table hb_tab_obj= getTable(pack_info_tab);

		getpkInfoFamilyMap();

		LOGGER.info("start row: = "+start+" Stop row:="+stop);	

		Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		//scan.setBatch(numberOfRows);
		scan.setFamilyMap(pkInfoFamilyMap);
		Integer cnt =0; 
		scanner = hb_tab_obj.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{	cnt++;
			for (Cell cell : result.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchPackinfo_AP_UPD() func");

	}

	public static void searchPackinfo_AP(String RowPrefix) throws IOException{

		LOGGER.info("In searchPackinfo_AP() func");

		Table hb_tab_obj = getTable(pack_info_tab);

		getpkInfoFamilyMap();

		LOGGER.info("start row: = "+RowPrefix+"Prefix Filter:"+RowPrefix);	

		Scan scan = new Scan(Bytes.toBytes(RowPrefix)); 
		scan.setRowPrefixFilter(Bytes.toBytes(RowPrefix));
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		
		//scan.setBatch(numberOfRows);
		scan.setFamilyMap(pkInfoFamilyMap);
		Integer cnt =0;
		scanner = hb_tab_obj.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{	cnt++;
			for (Cell cell : result.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchPackinfo_AP() func");	

	}

	public static void searchPackinfo_UPD(String start, String stop) throws IOException{

		LOGGER.info("In searchPackinfo_UPD() func");

		Table hb_tab_obj_upd = getTable(packinfo_upd_idx_tab);
		Table hb_tab_obj_pk_info = getTable(pack_info_tab);

		//getpkInfoFamilyMap();

		LOGGER.info("start row: = "+start+"stop:"+stop);	

		Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
		//scan.setRowPrefixFilter(Bytes.toBytes());
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		//scan.setBatch(numberOfRows);
		scan.addColumn(cf_idx, cf_idx_b);
		scan.addColumn(cf_idx, cf_idx_g);
		//scan.setFamilyMap(pkInfoFamilyMap);
		//int mm = 500;
		Integer cnt =0;
		ResultScanner scanner_upd = hb_tab_obj_upd.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner_upd.next(); result != null; result = scanner_upd.next())
		{	cnt ++;
			//String row = Bytes.toString(result.getValue(cf_idx,cf_idx_b)).concat(Bytes.toString(result.getValue(cf_idx,cf_idx_g)));
			//System.out.printf("key recived from UPD IDX : %s",row);
			Get get1 = new Get(Bytes.toBytes(Bytes.toString(result.getValue(cf_idx,cf_idx_b)).concat(Bytes.toString(result.getValue(cf_idx,cf_idx_g)))));
			//Query q ;
			Result getResult = hb_tab_obj_pk_info.get(get1);

			for (Cell cell : getResult.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
	/*		
			mm --;
			if(mm ==0)
			{
				System.out.printf("reached 500 gets on master table");
				break;
			}
			System.out.printf("**************************************************************");
	 */
		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchPackinfo_UPD() func");	

	}

	public static void searchPackinfo_UPD_getAll(String start, String stop) throws IOException{

		LOGGER.info("In searchPackinfo_UPD_getAll() func");

		Table hb_tab_obj_upd = getTable(packinfo_upd_idx_tab);
		Table hb_tab_obj_pk_info = getTable(pack_info_tab);

		//getpkInfoFamilyMap();

		LOGGER.info("start row: = "+start+"stop:"+stop);	

		Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop)); 
		//scan.setRowPrefixFilter(Bytes.toBytes());
		int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		//scan.setBatch(numberOfRows);
		scan.addColumn(cf_idx, cf_idx_b);
		scan.addColumn(cf_idx, cf_idx_g);
		//scan.setFamilyMap(pkInfoFamilyMap);
	
		List<Get> gets = new ArrayList<Get>();  
		ResultScanner scanner_upd = hb_tab_obj_upd.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		Result[] Allresult = scanner_upd.next(numberOfRows);
		Integer i = Allresult.length ;
		LOGGER.info("Count of Result: "+ i );
		while(i!=0)
		{ 	
			gets.add(new Get(Bytes.toBytes(Bytes.toString(Allresult[i].getValue(cf_idx,cf_idx_b)).concat(Bytes.toString(Allresult[i].getValue(cf_idx,cf_idx_g))))));

			Result[] getResult = hb_tab_obj_pk_info.get(gets);

			Integer j = Allresult.length ;

			while (j!=0)
				for (Cell cell : getResult[j].listCells()) {
					//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					//String value = Bytes.toString(CellUtil.cloneValue(cell));
					System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
				}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Exit from searchPackinfo_UPD_getAll() func");	

	}
	
	public static void searchProbe_VIN(String RowPrefix) throws IOException{

		LOGGER.info("In searchProbe_VIN() func");

		Table hb_tab_obj = getTable(blms_probe_tab);

		//getpkInfoFamilyMap();

		LOGGER.info("start row: = "+RowPrefix+"Prefix Filter:"+RowPrefix);	

		Scan scan = new Scan(Bytes.toBytes(RowPrefix)); 
		scan.setRowPrefixFilter(Bytes.toBytes(RowPrefix));
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		scan.addFamily(cf_cmn);
		scan.addFamily(cf_t);
		//scan.setBatch(numberOfRows);
		//scan.setFamilyMap(pkInfoFamilyMap);
		Integer cnt =0;
		scanner = hb_tab_obj.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{	cnt++;
			for (Cell cell : result.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchProbe_VIN() func");	

	}
	
	public static void searchProbe_VIN_TRIP(String RowPrefix) throws IOException{

		LOGGER.info("In searchProbe_VIN_TRIP() func");

		Table hb_tab_obj = getTable(blms_probe_tab);

		//getpkInfoFamilyMap();

		LOGGER.info("start row: = "+RowPrefix+"Prefix Filter:"+RowPrefix);	

		Scan scan = new Scan(Bytes.toBytes(RowPrefix)); 
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		//Filter Cfilter = new SingleColumnValueFilter(cf_t, cf_t_n, CompareOp.EQUAL, new SubstringComparator("TRIP"));
		Filter Cfilter = new SingleColumnValueFilter(cf_t, cf_t_n, CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("TRIP")));
		allFilters.addFilter(Cfilter);
		
		//Filter CPfilter = new ColumnPrefixFilter(Bytes.toBytes("TRIP"))
		//Filter CPfilter = new FamilyFilter(=,'binaryprefix:t');
		//new FamilyFilter(=, )
		Filter Pfilter = new PrefixFilter(Bytes.toBytes(RowPrefix));
		allFilters.addFilter(Pfilter);
		
		//scan.setRowPrefixFilter(Bytes.toBytes(RowPrefix));
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		//scan.addFamily(cf_cmn);
		//scan.addFamily(cf_t);
		//scan.addColumn(cf_cmn, cf_cmn_k);
		//scan.addColumn(cf_cmn, cf_cmn_l);
		//scan.addColumn(cf_cmn, cf_cmn_b);
		//scan.addColumn(cf_cmn, cf_cmn_c);
		//scan.addColumn(cf_cmn, cf_cmn_d);
		scan.setFilter(allFilters);
		//scan.doLoadColumnFamiliesOnDemand();
			
		//scan.setBatch(numberOfRows);
		//scan.setFamilyMap(pkInfoFamilyMap);
		Integer cnt =0;
		scanner = hb_tab_obj.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{	cnt++;
			//System.out.printf("\n\nCMN: %s:%s:%s:%s:%s\n\n Printing Trip", Bytes.toString(result.getValue(cf_cmn, cf_cmn_k)), Bytes.toString(result.getValue(cf_cmn, cf_cmn_l)), Bytes.toString(result.getValue(cf_cmn, cf_cmn_b)), Bytes.toString(result.getValue(cf_cmn, cf_cmn_c)), Bytes.toString(result.getValue(cf_cmn, cf_cmn_d)));
			for (Cell cell : result.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchProbe_VIN_TRIP() func");	

	}
	
	
	public static void searchProbe_VIN_TRIP_AllResultList(String RowPrefix) throws IOException{

		LOGGER.info("In searchProbe_VIN_TRIP_AllResultList() func");

		Table hb_tab_obj = getTable(blms_probe_tab);

		//getpkInfoFamilyMap();

		LOGGER.info("start row: = "+RowPrefix+"Prefix Filter:"+RowPrefix);	

		Scan scan = new Scan(Bytes.toBytes(RowPrefix)); 
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		//Filter Cfilter = new SingleColumnValueFilter(cf_t, cf_t_n, CompareOp.EQUAL, new SubstringComparator("TRIP"));
		//allFilters.addFilter(Cfilter);
		
		//Filter CPfilter = new ColumnPrefixFilter(Bytes.toBytes("TRIP"))
		//Filter CPfilter = new FamilyFilter(=,'binaryprefix:t');
		//new FamilyFilter(=, )
		Filter Pfilter = new PrefixFilter(Bytes.toBytes(RowPrefix));
		allFilters.addFilter(Pfilter);
		
		//scan.setRowPrefixFilter(Bytes.toBytes(RowPrefix));
		//int numberOfRows = Integer.MAX_VALUE;
		//scan.setCaching(numberOfRows);
		scan.setCacheBlocks(true);
		//scan.addFamily(cf_cmn);
		scan.addFamily(cf_t);
		
		scan.setFilter(allFilters);
		scan.doLoadColumnFamiliesOnDemand();
			
		//scan.setBatch(numberOfRows);
		//scan.setFamilyMap(pkInfoFamilyMap);
		Integer cnt =0;
		scanner = hb_tab_obj.getScanner(scan);

		LOGGER.info("Query StartTime(miliis): "+System.currentTimeMillis());	
		//Result[] result = scanner.next(numberOfRows);
		for (Result result = scanner.next(); result != null; result = scanner.next())
		{	cnt++;
			System.out.printf("\n\nCMN: %s:%s", Bytes.toString(result.getValue(cf_cmn, cf_cmn_k)), Bytes.toString(result.getValue(cf_cmn, cf_cmn_g)));
			for (Cell cell : result.listCells()) {
				//String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				//String value = Bytes.toString(CellUtil.cloneValue(cell));
				
				System.out.printf("%s:%s", Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}

		}
		LOGGER.info("Query EndTime(miliis): "+System.currentTimeMillis());	
		LOGGER.info("Count of Result: "+ cnt +"Exit from searchProbe_VIN_TRIP_AllResultList() func");	

	}
	
	public static void testPVLCPerformanceST() throws IOException
	{
	LOGGER.info("Inside of testPVLCPerformanceST()");	

	//String sampleVIN = "HNV37-550014";
	String sampleVIN = "vin15";
	String sampleNissanPack = "230JUZ1845031110    ";
	
	LOGGER.info("sampleVIN "+sampleVIN);	
	LOGGER.info("sampleNissanPack: "+sampleNissanPack);	
	
	String blms_pack_info_vin_idx = "blms_pack_info_vin_idx";
	String blms_pack_info_pack_map = "blms_pack_info_pack_map";
	Map<byte[], byte[]> VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	Map<byte[], byte[]> PACK_MAP_TBL = new HashMap<byte[], byte[]>();
	
	//LOGGER.log(Level.INFO, "Inside Of pvlcApply() for Pack: " + sampleVIN);
	//record to get existing mapping for pack change check : last PVLC check for VIN--> nissanpack	
	//LOGGER.info("VIN_IDX Query Start(miliis): "+System.currentTimeMillis());
	long vin_start = System.currentTimeMillis();
	//Result vin_idx_result = getLatestRecord(blms_pack_info_vin_idx, sampleVIN);
	Result vin_idx_result = getLatestRecordNotReversed(blms_pack_info_vin_idx, sampleVIN);
	LOGGER.info("Total VIN_IDX Query (Second): "+ (System.currentTimeMillis() - vin_start)/1000);	
	//LOGGER.info("VIN_IDX Query End(miliis): "+System.currentTimeMillis());	
	if (vin_idx_result != null) {
		VIN_IDX_TBL = vin_idx_result.getFamilyMap(cf_idx);
	} 
	//LOGGER.info("PACK MAP Query Start(miliis): "+System.currentTimeMillis());	
	long map_start = System.currentTimeMillis();
	//record to get existing mapping for pack change check : last PVLC check for  nissanpack --> vin
	//Result pack_map_result = getLatestRecord(blms_pack_info_pack_map, sampleNissanPack);
	Result pack_map_result = getLatestRecordNotReversed(blms_pack_info_pack_map, sampleNissanPack);
	LOGGER.info("Total MAP_IDX Query (Second): "+ (System.currentTimeMillis() - map_start)/1000);
	//LOGGER.info("PACK MAP Query End(miliis): "+System.currentTimeMillis());	
	
	if (pack_map_result != null) { //last PVLC or NPC already arrived.
		LOGGER.log(Level.INFO, "Pack found in PACK_MAP(last PVLC or NPC already arrived): " + sampleNissanPack);
		PACK_MAP_TBL = pack_map_result.getFamilyMap(cf_idx);
		LOGGER.log(Level.INFO, "PACK_MAP Entry (existing key): " + Bytes.toString(pack_map_result.getRow()));
		String alliance_pack = Bytes.toString(PACK_MAP_TBL.get(cf_cmn_b));
		String packmap_tbl_overseasBuyerCode = Bytes.toString(PACK_MAP_TBL.get(Bytes.toBytes("99"))); //set in static
		String packmap_tbl_BatterySettingDate = Bytes.toString(PACK_MAP_TBL.get(Bytes.toBytes("f"))); //set in static
		String packmap_tbl_vin = Bytes.toString(PACK_MAP_TBL.get(Bytes.toBytes("d")));
		LOGGER.log(Level.INFO, "alliance_pack: "+alliance_pack);
		LOGGER.log(Level.INFO, "packmap_tbl_overseasBuyerCode: "+packmap_tbl_overseasBuyerCode);
		LOGGER.log(Level.INFO, "packmap_tbl_BatterySettingDate: "+packmap_tbl_BatterySettingDate);
		LOGGER.log(Level.INFO, "packmap_tbl_vin: "+packmap_tbl_vin);
		LOGGER.info("testPVLCPerformanceST END ...............");	
		
		LOGGER.info("test Data deletion started.. ");	
		List<Delete> DeleteRowListVin = new ArrayList<>();
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5500142018-06-26 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5500142018-06-26 11:15:37.433");
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5500142018-06-27 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5500142018-06-27 11:15:37.433");
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5300392018-06-26 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5300392018-06-26 11:15:37.433");
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5300392018-06-26 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5300392018-06-26 11:15:37.433");
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5300392019-06-26 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5300392019-06-26 11:15:37.433");
		DeleteRowListVin.add(new Delete(Bytes.toBytes("HNV37-5300392019-06-26 11:15:37.433")));
		LOGGER.info("Deleted VINIDX KEY: HNV37-5300392019-06-26 11:15:37.433");
		deleteFromTable(blms_pack_info_vin_idx, DeleteRowListVin);
		LOGGER.info("test Data deletion finished.... ");
	}

	}
	
	public static void main(String[] args) throws ServiceException,IOException {

		try {
			//get connections
			getResources();
			
	/*		//search AP+UPD
			String start = "295B03NA0A JT111CC000796 2010-01-01";
			String stop = "295B03NA0A JT111CC000796 2018-06-01";
			searchPackinfo_AP_UPD(start, stop);	

			String rowPrefix= "295B03NK0A JT112AG001311 ";
			searchPackinfo_AP(rowPrefix);  

			//search UPD
			String updStart = "2013-01-01";
			String updStop = "2016-01-01";			
			searchPackinfo_UPD(updStart, updStop);

			//search using in memory all values after lookup
			//searchPackinfo_UPD_getAll(updStart, updStop);

			//Search Probe using Vin
			String RowPrefix ="ZE0-017442";
			searchProbe_VIN(RowPrefix);
			
			//Search Probe-TRIP using VIN
			searchProbe_VIN_TRIP(RowPrefix);
			*/
			
			//getpkInfoFamilyMap();
			
			testPVLCPerformanceST();


		}finally{

			LOGGER.info("In Finally block of Main(),  EndTime(miliis): "+System.currentTimeMillis());	
		}
	}
}





