/*
 * compile with
 * javac -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PVLU.java
 * nohup java -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PVLU 'hdfs://hahdfsqa/user/asp20571bdp/BLMB0102/PVLU/input/111111_20200201130000/VALID_PVLU_INPUT_FOR_JAVA/part-v009-o000-r-00000' '2020-02-01 13:00:00.000' '20200201130000' > PVLU_NEW.log 2>&1 &
 * 
 */
package blms.hbase.test.inprogress;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class PVLU_OLD {
	
	static Map<byte[], byte[]> PVLU_IN_FEILDS = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> BLMS_PROBE_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_UPD_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_MAP_TBL = new HashMap<byte[], byte[]>();
	static Map<Integer, String> JOB_DETAILS = new HashMap<Integer, String>(); //Collect the success or error case of each pvlu record
	static int line_cnt = 0;
	private static String new_UPD = "2020-02-01 13:00:00.000"; 
	private static String V_READ_DATE = "20200201130000";
	private static FileHandler fh;
	
	//private static String wf_id = "0000";
	private static String ProbeKey ;
	private static String newSearch1Key ;
	private static String oldSearch1Key ;
	private static String newSearch2Key ;
	private static String oldSearch2Key ;
	private static String newSearch3Key ;
	private static String oldSearch3Key ;
	private final static byte [] cp_event_id = Bytes.toBytes("Pack_Vehicle_Linkage_Update");

	private final static byte[] cf_pk = Bytes.toBytes("pk");
	private final static byte[] cf_idx = Bytes.toBytes("idx");
	private final static byte[] cf_cmn = Bytes.toBytes("cmn");
	private final static byte[] cf_cp = Bytes.toBytes("cp");
	//private final static byte[] cf_cmn = Bytes.toBytes("cmn");
	private final static byte[] pkinfo_blms_id = Bytes.toBytes("a");
	//private final static byte[] pkinfo_blms_id = Bytes.toBytes("5");
	private final static byte[] pkinfo_vin = Bytes.toBytes("d");
	//private final static byte[] pkinfo_vin = Bytes.toBytes("8");
	private final static byte[] pkinfo_upd = Bytes.toBytes("g");
	//private final static byte[] pkinfo_upd = Bytes.toBytes("19");
	private final static byte[] pkinfo_orignalKey = Bytes.toBytes("k");
	//private final static byte[] pkinfo_orignalKey = Bytes.toBytes("3");
	private final static byte[] pkinfo_alliancePk = Bytes.toBytes("b");
	private final static byte[] pkinfo_pvdate = Bytes.toBytes("f");
	private final static byte[] pkinfo_nissanPack = Bytes.toBytes("c");
	//private final static byte[] pkinfo_pvdate = Bytes.toBytes("3");
    //pkinfo_rmvdate wil be always 100 since this is a column taken from reserve
	private final static byte[] pkinfo_rmvdate = Bytes.toBytes("100");
	//private final static byte[] probe_BLMSPackID = Bytes.toBytes("a");
	private final static byte[] pkinfo_PackDateKey = Bytes.toBytes("e");
	private final static byte[] pkinfo_Timestamp = Bytes.toBytes("l");
	private final static byte[] pkinfo_event_id = Bytes.toBytes("n");
	private final static byte[] CP_V_COL_001 = Bytes.toBytes("3");
	private final static byte[] CP_V_COL_002 = Bytes.toBytes("4");
	private final static byte[] cp_VFReadDate = Bytes.toBytes("q");
	private final static String hbaseNameSpace = "ASP20571";
	private final static String blms_probe = "blms_pvlu_test_probe";
	private final static String blms_probe_search1 = "blms_pvlu_test_probe_search1";
	private final static String blms_probe_search2 = "blms_pvlu_test_probe_search2";
	private final static String blms_probe_search3 = "blms_pvlu_test_probe_search3";
	private final static String blms_pack_info = "blms_pvlu_test_pack_info";
	private final static String blms_pack_info_upd_idx = "blms_pvlu_test_pack_info_upd_idx";
	private final static String blms_pack_info_vin_idx = "blms_pvlu_test_pack_info_vin_idx";
	//private final static String blms_pack_info_pack_map = "blms_pvlu_test_pack_info_pack_map";
	private final static String blms_cache_processed = "blms_pvlu_test_cache_processed";
	private final static Logger LOGGER = Logger.getLogger(PVLU_OLD.class.getName());	
	static Connection conn = null;
/*	
	public static Connection getHbaseConnection() throws IOException{
		// This block configure the logger with handler and formatter  


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
	        //Create once and keep it arround
	        conn = ConnectionFactory.createConnection(configuration);

	        return conn;

	} //end constructor */
/*	
static Table hbaseConnect(String tbl_name, Connection conn) throws IOException
	{
		

		TableName tbl = TableName.valueOf(hbaseNameSpace+":"+tbl_name);
		Table tblConn =  conn.getTable(tbl);
		return (tblConn);	
}//end hbaseConnect() */

 /*
public static ResultScanner getPackInfoScanner(String pvlu_alliancePack, Connection conn) throws IOException {
	   //Table blms_pack_info_tbl = hbaseConnect(blms_pack_info, conn);
	   TableName blms_pack_info_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
	   Table blms_pack_info_tblConn =  conn.getTable(blms_pack_info_tbl);
	   Scan scan = new Scan(); 
	   LOGGER.info("Set start Row for scan: "+pvlu_alliancePack);
	   //scan.setStartRow(Bytes.toBytes(token[1])); //token[1] <- AP
	   FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	   allFilters.addFilter(new PrefixFilter(Bytes.toBytes(pvlu_alliancePack)));
	   scan.setFilter(allFilters);
	   scan.setReversed(true); // read the latest available key and values for this AP
	   scan.setMaxResultSize(1);
	   LOGGER.info("scan object to string before execute: "+scan.toString());
     	ResultScanner scanner = blms_pack_info_tblConn.getScanner(scan);
     	return scanner;
} */

public static ResultScanner getVinIdxScanner(String pvlu_vin, Connection conn) throws IOException {
  // Table blms_pack_info_vin_idx_tbl = hbaseConnect(blms_pack_info_vin_idx,conn);
	TableName blms_pack_info_vin_idx_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_vin_idx);
	Table blms_pack_info_vin_idx_tblConn =  conn.getTable(blms_pack_info_vin_idx_tbl);
	   Scan scan = new Scan(); 
	   LOGGER.info("Set start Row for scan: "+pvlu_vin);
	   //scan.setStartRow(Bytes.toBytes(token[1])); //token[1] <- AP
	   FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	   allFilters.addFilter(new PrefixFilter(Bytes.toBytes(pvlu_vin)));
	   scan.setFilter(allFilters);
	   scan.setReversed(true); // read the latest available key and values for this AP
	   scan.setMaxResultSize(1);
	   LOGGER.info("scan object to string before execute: "+scan.toString());
     	ResultScanner scanner = blms_pack_info_vin_idx_tblConn.getScanner(scan);
     	return scanner;
}




public static ResultScanner getProbeScanner(String pvlu_vin, String pvlu_setpvDate, Connection conn) throws IOException {
	   TableName blms_probe_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_probe);
	   Table blms_probe_tblConn =  conn.getTable(blms_probe_tbl);
	   //Table blms_probe_tbl = hbaseConnect(blms_probe,conn);
	   Scan scan = new Scan(); 
	   LOGGER.info("Set start Row for scan: "+pvlu_vin+pvlu_setpvDate);
	   scan.setStartRow(Bytes.toBytes(pvlu_vin+pvlu_setpvDate));
	   //pvlu_vin+pvlu_setpvDate <- VIN+PVdate(new attaching pvdate, utc converted)
	   FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	   allFilters.addFilter(new PrefixFilter(Bytes.toBytes(pvlu_vin)));
	   scan.setFilter(allFilters);
	   //scan.setReversed(true); // read the latest available key and values for this AP
	   //scan.setMaxResultSize(1000000000);
	   LOGGER.info("scan object to string before execute: "+scan.toString());
  	ResultScanner scanner = blms_probe_tblConn.getScanner(scan);
  	return scanner;
}

public static Map<Integer, String> removeApply(String[] token, Connection conn) throws IOException {
		//ResultScanner scanner = getPackInfoScanner(token[1],conn); //toke[1] <- AP
		LOGGER.log(Level.INFO,"Inside Of removeApply() for removing AlliancePack: "+token[1]+" & Vin: "+token[4]);
		TableName blms_pack_info_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
		Table blms_pack_info_tblConn =  conn.getTable(blms_pack_info_tbl);
		Scan scan = new Scan(); 
		//LOGGER.info("Set start Row for scan: "+token[1]);
		//scan.setStartRow(Bytes.toBytes(token[1])); //token[1] <- AP
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[1])));
		scan.setFilter(allFilters);
		scan.setReversed(true); // read the latest available key and values for this AP
		scan.setMaxResultSize(1);
		LOGGER.log(Level.INFO,"scan object to string before execute: "+scan.toString());
	    ResultScanner scanner = blms_pack_info_tblConn.getScanner(scan);
		Result result = scanner.next();
		scanner.close(); //Packinfo scanner close
		if(result != null)
    	 {  PACK_INFO_TBL = result.getFamilyMap(cf_pk);
    	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
    	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
    	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
    	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get( pkinfo_orignalKey)));
    	 	String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
    	 	String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
    	 	LOGGER.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
    	 	LOGGER.log(Level.INFO,"cond2_pkinfo_vin: "+cond2_pkinfo_vin);
    	 	// LOGGER.log(Level.INFO,"Outside getFamilMap Accessed value for orignal key: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
			// LOGGER.log(Level.INFO,"Outside getFamilMap Accessed value for pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
    	 //if(result.containsNonEmptyColumn(cf_pk, pkinfo_blms_id))
      	 if((!cond1_blms_pack_id.isEmpty()) || cond1_blms_pack_id.length()!=0)	
    	 	{
      		 LOGGER.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
    	 		//blms id present, arrived alliance pack present.: true
      		 LOGGER.log(Level.INFO,"Next check arrived vin really attached to this pack.."+cond2_pkinfo_vin);
    	 	 	//token[4] <-Arrived vin
    	 		if(cond2_pkinfo_vin.compareTo(token[4])==0){
    	 			//Arrived "VIN" is also attached to arrived Alliance pack
    	 			LOGGER.log(Level.INFO,"Arrived Vin is Matching, Arrived VIN: "+token[4]); 
    	 			//PACK_INFO_TBL = result.getFamilyMap(cf_pk);  
    	 			// token[1] <- AP
    	 			// get the "token[1]+pkinfo_upd" values from packinfo latest record 
    	 			// Use PUT on : upd_idx tbl  "new UPD+token[1]"=> b:token[1]; g:new UPD
    	 			// Use PUT on : vin_idx_tbl  "token[4]+new UPD"=> b:token[1]; g:new UPD
    	 			// Create New Records in pack-info: with new key(token[1]+new UPD) and pk:vin=null,pk:pvdate=null,pk:removedate=removeDate
     	 			//LOGGER.log(Level.INFO,"Inside getFamilMap Accessed value for orignal key: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
    	 			//LOGGER.log(Level.INFO,"Inside getFamilMap Accessed value for pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
    	 			//LOGGER.log(Level.INFO,"Inside getFamilMap Accessed value for pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
    	 			//LOGGER.log(Level.INFO,"Deatching Vin: "+ Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)) + " From the Alliance Pack: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
  /*
    	 				b	-> Alliance Pack1 -> null
    	 				g	-> T_UPD_DATE1 -> new_UPD
    	 				f	-> Battery setting Date -> null
    	 				c	-> N_Pack(To check PVLC) -> null
    	 				d	-> VIN -> same VIN
 */					LOGGER.log(Level.INFO,"prepare new record for:vin_idx_tbl; Detach VIN.."+cond2_pkinfo_vin);
    	 			//prepare new record for:vin_idx_tbl; Detach VIN
 					byte [] newVinIdxKey = Bytes.toBytes(token[4].concat(new_UPD));
    	 			Put NewVinIdx_kv = new Put(newVinIdxKey); //token[4] <-Arrived VIN
    	 			NewVinIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
    	 			NewVinIdx_kv.addColumn(cf_idx,pkinfo_vin,PACK_INFO_TBL.get(pkinfo_vin));
    	 			LOGGER.log(Level.INFO,Bytes.toString(pkinfo_upd),new_UPD);
    	 			LOGGER.log(Level.INFO,Bytes.toString(pkinfo_vin),Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
    	 				
    	   	 		TableName blms_pack_info_vin_idx_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_vin_idx);
		   	 		Table blms_pack_info_vin_idx_tblConn =  conn.getTable(blms_pack_info_vin_idx_tbl);
		   	 		putInTable(blms_pack_info_vin_idx_tblConn,NewVinIdx_kv);
		   	 		blms_pack_info_vin_idx_tblConn.close();
    	 			//Table blms_pack_info_vin_idx_tbl = hbaseConnect(blms_pack_info_vin_idx,conn);
    	 			//putInTable(blms_pack_info_vin_idx_tbl,NewVinIdx_kv);
    	 			//end vin_idx Updated for New record insert
		   	 		LOGGER.log(Level.INFO,"vin_idx Updated for New record insert.."+Bytes.toString(newVinIdxKey));
    	 			//prepare new record for:upd_idx_tbl
		   	 		LOGGER.log(Level.INFO,"Prepare new record for:upd_idx_tbl..");
		   	 		byte [] newUpdIdxKey = Bytes.toBytes(new_UPD.concat(token[1]));
    	 				Put NewUpdIdx_kv = new Put(newUpdIdxKey);  //token[1] <- AP
    	 				NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
    	 				NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,PACK_INFO_TBL.get(pkinfo_orignalKey));
    	 				LOGGER.log(Level.INFO,Bytes.toString(pkinfo_upd),new_UPD);
    	 				LOGGER.log(Level.INFO,Bytes.toString(pkinfo_alliancePk),Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
    	 				
		   	 		    TableName blms_pack_info_upd_idx_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_upd_idx);
		   	 		    Table blms_pack_info_upd_idx_tblConn =  conn.getTable(blms_pack_info_upd_idx_tbl);
		   	 		    putInTable(blms_pack_info_upd_idx_tblConn,NewUpdIdx_kv);
		   	 		    blms_pack_info_upd_idx_tblConn.close();
    	 				//Table blms_pack_info_upd_idx_tbl = hbaseConnect(blms_pack_info_upd_idx,conn);
    	 				//putInTable(blms_pack_info_upd_idx_tbl,NewUpdIdx_kv);
    	 				//end UPD_idx Updated for New record insert
		   	 		    LOGGER.log(Level.INFO,"End UPD_idx Updated for New record insert.."+Bytes.toString(newUpdIdxKey));
    	 		
    	 				//Preparing new record for packInfo:
		   	 		    LOGGER.log(Level.INFO,"Preparing new record for packInfo..");
    	 				PACK_INFO_TBL.put(pkinfo_vin,null);
    	 				PACK_INFO_TBL.put(pkinfo_pvdate,null);
    	 				PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(new_UPD));
    	 				PACK_INFO_TBL.put(pkinfo_rmvdate,Bytes.toBytes(token[2])); //token[2] <- battery remove date from pvlu rec
    	 				PACK_INFO_TBL.entrySet();
    	 				byte [] newPackInfoKey = Bytes.toBytes(token[1].concat(new_UPD));
      	 				Put NewPackInfo_kv = new Put(newPackInfoKey); //token[1] <- AP
						for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
    	 				    byte[] key = entry.getKey();
    	 				    byte[] value = entry.getValue();
    	 				    LOGGER.log(Level.INFO,Bytes.toString(key),Bytes.toString(value));
    	 				    NewPackInfo_kv.addColumn(cf_pk,key, value);
						}
		   	 		    TableName blms_pack_info_tbl_out = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
		   	 		    Table blms_pack_info_tbl_outConn =  conn.getTable(blms_pack_info_tbl_out);
		   	 		    putInTable(blms_pack_info_tbl_outConn,NewPackInfo_kv);
		   	 		    blms_pack_info_tbl_outConn.close();
      	 				//Table blms_pack_info_tbl = hbaseConnect(blms_pack_info,conn);
      	 				//putInTable(blms_pack_info_tbl,NewPackInfo_kv);
      	 				//end of new record for packInfo insert 
		   	 		    LOGGER.log(Level.INFO,"End of new record for packInfo insert.."+Bytes.toString(newPackInfoKey));
      	 				JOB_DETAILS.put(line_cnt,"success");
      	 				
    	 		}else {
    	 			LOGGER.log(Level.INFO,"ERROR CASE: Arrived VIN is not attached to Arrived Rmoving Pack.."+token[1]);
    	 			JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived VIN is not attached to Arrived Rmoving Pack.."+token[1]);
    	 		}
    	 	}else{
    	 		LOGGER.log(Level.INFO,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
    	 		JOB_DETAILS.put(line_cnt,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
    	 	}
    	
    	 }else {
    	 	LOGGER.log(Level.INFO,"ERROR CASE: Removing Pack is not Present in PackInfo Table.."+token[1]);
    		 JOB_DETAILS.put(line_cnt,"ERROR CASE: Removing Pack is not Present in PackInfo Table.."+token[1]);
    	 }
		return JOB_DETAILS;
		
}//end removeApply()


public static Map<Integer, String> setApply(String[] token, Connection conn) throws IOException  {
	LOGGER.log(Level.INFO,"Inside Of setApply() for attaching AlliancePack: "+token[1]+" & Vin: ",token[4]);
	//1. Check AlliancePack, BlmsPack present for arrived event in "pk_info" tbl.
	//2. if 1 true, then check "vin" feild is empty for this arrived pack in "pk_info" tbl.
		//(arrived pack is not linked to any other vin.)
	//3. if 2 true, then check, Arrived "vin" is not already liked with any pack. (in vin_idx tbl)
	//4. if all above true, then apply the Set to packinfo:
		//4.1: After pk_info, update the : upd_idx, vin_idx_tbl,
			   //Conditional (probe:timestamp > pkinfo:pvdate ) - probe:cmn, search1, search2, search3.
	
	//LOGGER.log(Level.INFO,"Inside Of removeApply() for attaching AlliancePack: "+token[1]+" & Vin: ",token[4]);
	TableName blms_pack_info_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
	Table blms_pack_info_tblConn =  conn.getTable(blms_pack_info_tbl);
	Scan scan = new Scan(); 
	//LOGGER.info("Set start Row for scan: "+token[1]);
	//scan.setStartRow(Bytes.toBytes(token[1])); //token[1] <- AP
	FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[1])));
	scan.setFilter(allFilters);
	scan.setReversed(true); // read the latest available key and values for this AP
	scan.setMaxResultSize(1);
	LOGGER.log(Level.INFO,"scan object to string before execute: "+scan.toString());
    ResultScanner scanner = blms_pack_info_tblConn.getScanner(scan);
	Result result = scanner.next();
	scanner.close(); //Packinfo scanner close
	blms_pack_info_tblConn.close();
	if(result != null)
	 {  PACK_INFO_TBL = result.getFamilyMap(cf_pk);
	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
	 	LOGGER.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get( pkinfo_orignalKey)));
	 	String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
	 	String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
	 	String cond3_attaching_vin = token[4]; //arrived attachinging VIN
	 	LOGGER.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
	 	LOGGER.log(Level.INFO,"cond2_pkinfo_vin: "+cond2_pkinfo_vin);
	 	LOGGER.log(Level.INFO,"cond3_attaching_vin: "+cond3_attaching_vin);
	 	
	 //if(result.containsNonEmptyColumn(cf_pk, pkinfo_blms_id))
	 	if(cond1_blms_pack_id !=null && cond1_blms_pack_id.length()!=0)	
	 	{
  		 LOGGER.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
     	 LOGGER.log(Level.INFO,"Next check : vin columns is null or not present for arrived attaching pack in pkinfo tbl: "+token[1]);
   		 if(cond2_pkinfo_vin.length()==0 || cond2_pkinfo_vin ==null )	
   	 		{
   			LOGGER.log(Level.INFO,"Arrived Pack is not attached to any other VIN: "+token[1]);
   		 	LOGGER.log(Level.INFO,"Next check arrived VIN is not attached to any other Pack: "+token[4]);
   		    
   			TableName blms_pack_info_vin_idx_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_vin_idx);
   			Table blms_pack_info_vin_idx_tblConn =  conn.getTable(blms_pack_info_vin_idx_tbl);
   			Scan vinIdxScan = new Scan(); 
   			LOGGER.info("Set start Row for scan: "+token[4]); //arrived VIN
   			//scan.setStartRow(Bytes.toBytes(token[4])); 
   			FilterList vinIdxallFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
   			vinIdxallFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4])));
   			vinIdxScan.setFilter(vinIdxallFilters);
   			vinIdxScan.setReversed(true); // read the latest available key and values for this AP
   			vinIdxScan.setMaxResultSize(1);
   			LOGGER.info("scan object to string before execute: "+vinIdxScan.toString());
   		    ResultScanner vinIdxScanner = blms_pack_info_vin_idx_tblConn.getScanner(vinIdxScan);
   			 //logic to check if VIN is already attached to anther nissan pack:
   			// ResultScanner vinIdxScanner = getVinIdxScanner(token[4],conn);
   		    Result VinIdxresult = vinIdxScanner.next(); //retrive latest record for this VIN if present.
   		    vinIdxScanner.close();
   		    blms_pack_info_vin_idx_tblConn.close();
   			 	if(VinIdxresult != null)
   			 	{
   			 	VIN_IDX_TBL = VinIdxresult.getFamilyMap(cf_idx);
   			 	LOGGER.log(Level.INFO,"latest key value from VIN_IDX TBL for pvlu_vin: "+Bytes.toString(VIN_IDX_TBL.get(pkinfo_vin)));
   			 	LOGGER.log(Level.INFO,"latest key value from VIN_IDX TBL for already attached  NissanPack: "+Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack)));
   			 		//Check AP is Attached or Nissanpack attached(pvlc) to this VIN
   			 	String vinIdxcond1_pkinfo_nissanPack= Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack));
   			 	LOGGER.log(Level.INFO,"vinIdxcond1_pkinfo_nissanPack: "+vinIdxcond1_pkinfo_nissanPack);
   			 	String vinIdxcond2_alliancePk= Bytes.toString(VIN_IDX_TBL.get(pkinfo_alliancePk));
   			 	LOGGER.log(Level.INFO,"vinIdxcond2_alliancePk: "+vinIdxcond2_alliancePk);
   			 	//String l = null;
   			 	//if(((vinIdxcond1_pkinfo_nissanPack.isEmpty()) || vinIdxcond1_pkinfo_nissanPack.length()==0 || vinIdxcond1_pkinfo_nissanPack ==null) ||
   		   		//	 				((vinIdxcond2_alliancePk.isEmpty()) || vinIdxcond2_alliancePk.length()==0 || vinIdxcond2_alliancePk ==null)) {
   			 if(vinIdxcond1_pkinfo_nissanPack ==null || vinIdxcond2_alliancePk ==null) {
   			 				  LOGGER.log(Level.INFO,"Arrived Vin is not attached to another nissan Pack: "+token[4]);
   			 					//VIN_IDX_TBL = VinIdxresult.getFamilyMap(cf_idx);
   			 					//1. Update the PACKINFO tbl
   			   	 				//Preparing new record for packInfo:
   			 				LOGGER.log(Level.INFO,"Preparing new record for packInfo..");
   			   	 			PACK_INFO_TBL.put(pkinfo_vin,Bytes.toBytes(token[4]));  //set the arrived VIN
   			   	 			PACK_INFO_TBL.put(pkinfo_pvdate,Bytes.toBytes(token[2])); //set the Battery setting Date as pvdate
   			   	 			PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(new_UPD));   // new UPD date for record
   	
   			   	 			byte [] newPackInfoKey = Bytes.toBytes(token[1].concat(new_UPD));
   			   	 				//PACK_INFO_TBL.put(pkinfo_rmvdate,Bytes.toBytes(token[2])); //token[2] <- battery remove date from pvlu rec
   			   	 			Put NewPackInfo_kv = new Put(newPackInfoKey); //token[1] <- AP
   									for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
   			   	 				    	byte[] key = entry.getKey();
   			   	 				    	byte[] value = entry.getValue();
   			   	 				    	LOGGER.log(Level.INFO,Bytes.toString(key),Bytes.toString(value));
   			   	 				    	NewPackInfo_kv.addColumn(cf_pk,key, value);
   										}
   									
   				   	 		        TableName blms_pack_info_tbl_out = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
   				   	 		        Table blms_pack_info_tbl_outConn =  conn.getTable(blms_pack_info_tbl_out);
   				   	 		        putInTable(blms_pack_info_tbl_outConn,NewPackInfo_kv);
   				   	 		        blms_pack_info_tbl_outConn.close();
   			     	 				//Table blms_pack_info_tbl = hbaseConnect(blms_pack_info,conn);
   			     	 				//putInTable(blms_pack_info_tbl,NewPackInfo_kv);
   			     			     	//end of new record for packInfo insert 
   				   	 		        LOGGER.log(Level.INFO,"End of new record for packInfo insert:"+Bytes.toString(newPackInfoKey));
   			 					
   			 					//2. Update the UPD_IDX table
   				   	 			// Prepare new record for:upd_idx_tbl
   				   	 		        LOGGER.log(Level.INFO,"Prepare new record for:upd_idx_tbl: "+token[1]);
   				   	 		        byte [] newUpdKey = Bytes.toBytes(new_UPD.concat(token[1]));
   				   	 				Put NewUpdIdx_kv = new Put(newUpdKey);  //token[1] <- AP
   				   	 				NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
   				   	 				NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,PACK_INFO_TBL.get(pkinfo_orignalKey));
   				   	 				
   				   	 		        TableName blms_pack_info_upd_idx_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_upd_idx);
   				   	 		        Table blms_pack_info_upd_idx_tblConn =  conn.getTable(blms_pack_info_upd_idx_tbl);
   				   	 		        putInTable(blms_pack_info_upd_idx_tblConn,NewUpdIdx_kv);
   				   	 		        blms_pack_info_upd_idx_tblConn.close();
   				   	 				//Table blms_pack_info_upd_idx_tbl = hbaseConnect(blms_pack_info_upd_idx,conn);
   				   	 				//putInTable(blms_pack_info_upd_idx_tbl,NewUpdIdx_kv);
   								   	//end UPD_idx Updated for New record insert
   				   	 		        LOGGER.log(Level.INFO,"End UPD_idx Updated for New record insert: "+Bytes.toString(newUpdKey));
   			 					//3. Update the VIN_IDX tbl
   				   	 				
   				   	 			/*  b <- 	Alliance Pack1
   				   	 				g <- 	T_UPD_DATE1
   				   	 				f <-	Battery setting Date
   				   	 				c <-	N_Pack(To check PVLC)
   				   	 			    d <-	VIN  
   				   	 			 */
   				   	 		        LOGGER.log(Level.INFO,"Prepare new record for:vin_idx_tbl; Attach VIN: "+token[4]);
   				   	 				//Prepare new record for:vin_idx_tbl; 
   				   	 		        byte [] newVinKey = Bytes.toBytes(token[4].concat(new_UPD));
   				   	 				Put NewVinIdx_kv = new Put(newVinKey); //token[4] <-Arrived VIN
   				   	 				NewVinIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD)); //new_UPD
   				   	 				NewVinIdx_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(token[4])); //VIN column
   				   	 			    NewVinIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,Bytes.toBytes(token[1])); //Attached AP
   				   	 		        NewVinIdx_kv.addColumn(cf_idx,pkinfo_pvdate,Bytes.toBytes(token[2])); //Battery Setting Date
   				   	 		        NewVinIdx_kv.addColumn(cf_idx,pkinfo_nissanPack,PACK_INFO_TBL.get(pkinfo_nissanPack)); //NissanPack
   				   				
   				   	 		        //Table blms_pack_info_vin_idx_tbl = hbaseConnect(blms_pack_info_vin_idx,conn);
   				   	 		        //putInTable(blms_pack_info_vin_idx_tbl,NewVinIdx_kv);
   				   	 		        TableName blms_pack_info_vin_idx_tbl_out = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info_vin_idx);
   				   	 		        Table blms_pack_info_vin_idx_tbl_outConn =  conn.getTable(blms_pack_info_vin_idx_tbl_out);
   				   	 		        putInTable(blms_pack_info_vin_idx_tbl_outConn,NewVinIdx_kv);
   				   	 		        blms_pack_info_vin_idx_tbl_outConn.close();
   								  //end vin_idx Updated for New record insert
   				   	 		        LOGGER.log(Level.INFO,"vin_idx Updated for New record insert: "+Bytes.toString(newVinKey));
   				
   				   	 		//4. Enrich the Valid Probe & its idx tbls for this VIN-PACK attachment	
   				   	 		 LOGGER.log(Level.INFO,"blms_probe enrichment Started for VIN: "+token[4]);
   				   	 		
   				   	   TableName blms_probe_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_probe);
   					   Table blms_probe_tblConn =  conn.getTable(blms_probe_tbl);
   					   //Table blms_probe_tbl = hbaseConnect(blms_probe,conn);
   					   Scan probeScan = new Scan(); 
   					   LOGGER.log(Level.INFO,"Set start Row for scan: "+token[4]+token[2]);
   					   probeScan.setStartRow(Bytes.toBytes(token[4]+token[2]));
   					   //pvlu_vin+pvlu_setpvDate <- VIN+PVdate(new attaching pvdate, utc converted)
   					   FilterList probeAllFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
   					   probeAllFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4])));
   					   probeScan.setFilter(probeAllFilters);
   					   //scan.setReversed(true); // read the latest available key and values for this AP
   					   //scan.setMaxResultSize(1000000000);
   					   LOGGER.log(Level.INFO,"scan object to string before execute: "+probeScan.toString());
   					   ResultScanner ProbeScanner = blms_probe_tblConn.getScanner(probeScan);

   				  	
   				   	 		 //ResultScanner ProbeScanner = getProbeScanner(token[4], token[2], conn); //token[4] :vin, token[2]: pvdate
   				   	 		 List<Put> puts_probe = new ArrayList<Put>();
   				   	 		 List<Put> puts_probe_search1 = new ArrayList<Put>();
   				   	 		 List<Put> puts_probe_search2 = new ArrayList<Put>();
   				   	 		 List<Put> puts_probe_search3 = new ArrayList<Put>();
   				   	 		 List<Delete> delete_probe_search1 = new ArrayList<Delete>();
   				   	 		 List<Delete> delete_probe_search2 = new ArrayList<Delete>();
   				   	 		 List<Delete> delete_probe_search3 = new ArrayList<Delete>();
   				   	 		 for (Result ProbeResult = ProbeScanner.next(); ProbeResult != null; ProbeResult = ProbeScanner.next())
   				   	 		 	{
   				   	 			//4. Update the PROBE and respective idx tbl.
   				   	 			//LOGGER.info("Check condition for: Probe,search1,search2,search3 updates..");
   				   	 			//if(token[2].compareTo(anotherString) < 0) {
   				   	 			//	}
   				   	 			// for blms_probe, directly issue put for all rowprifix and start row matched.
   				   	 			 	BLMS_PROBE_TBL = ProbeResult.getFamilyMap(cf_cmn);
   				   	 			 	String ProbeKey = Bytes.toString(ProbeResult.getRow());
   				   	 			    LOGGER.log(Level.INFO,"Probe-RokwKey for Enrichment:  "+ProbeKey);
   				   	 			 	//Enrichment for "cmn cf"
   				   	 			 	Put NewProbe_kv = new Put(ProbeResult.getRow());  //VIN+TIMESTAMP
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_blms_id,PACK_INFO_TBL.get(pkinfo_blms_id));
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_alliancePk,PACK_INFO_TBL.get(pkinfo_alliancePk));
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_nissanPack,PACK_INFO_TBL.get(pkinfo_nissanPack));
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_vin,Bytes.toBytes(token[4])); //vin
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_PackDateKey,PACK_INFO_TBL.get(pkinfo_PackDateKey));
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_pvdate,Bytes.toBytes(token[2])); //BatterysettingDate
   				   	 			 	NewProbe_kv.addColumn(cf_cmn,pkinfo_upd,Bytes.toBytes(new_UPD));//new upd
   				   	 			 	//NewProbe_kv.addColumn(cf_cmn,pkinfo_orignalKey,Bytes.toBytes(new_UPD));
   				   	 			 	//NewProbe_kv.addColumn(cf_cmn,pkinfo_Timestamp,Bytes.toBytes(new_UPD));
   				   	 			 	puts_probe.add(NewProbe_kv);
   				   	 			 	//--end of probe enrichment
   				   	 				
   				   	 			 	//LOGGER.log(Level.INFO,"Preparing Corresponding Index Tbl Updates For PROBE KEY Started: "+ProbeKey);
   				   	 			 	String new_probe_vin = token[4]; //new vin
   				   	 			 	String new_probe_APack = Bytes.toString(PACK_INFO_TBL.get(pkinfo_alliancePk));
   				   	 			 	String new_probe_upd = new_UPD;
   				   	 			 	String new_probe_ts = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_Timestamp));		   	 			 	
   				   	 			 	//Start search1 updates using new key from probe: cmn cf
   				   	 			 	String newSearch1Key = new_probe_APack.concat(new_probe_upd).concat(new_probe_ts);
   				   	 			 	Put NewProbe_Search1_kv = new Put(Bytes.toBytes(newSearch1Key));  //VIN+TIMESTAMP
   				   	 			 	NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(new_probe_vin));
   				   	 			 	NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_Timestamp,Bytes.toBytes(new_probe_ts));
   				   	 			 	NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_probe_upd));
   				   	 			 	NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_alliancePk,Bytes.toBytes(new_probe_APack));
   				   	 			 	puts_probe_search1.add(NewProbe_Search1_kv);
   				   	 			 	//----end for search1
   				   	 			 	//Start search2 updates using new key from probe: cmn cf
   				   	 			 	String newSearch2Key = new_probe_upd.concat(new_probe_APack).concat(new_probe_ts);
   				   	 			 	Put NewProbe_Search2_kv = new Put(Bytes.toBytes(newSearch2Key));  //VIN+TIMESTAMP
   				   	 			 	NewProbe_Search2_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(new_probe_vin));
   				   	 			 	NewProbe_Search2_kv.addColumn(cf_idx,pkinfo_Timestamp,Bytes.toBytes(new_probe_ts));
   				   	 			 	NewProbe_Search2_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_probe_upd));
   				   	 			 	NewProbe_Search2_kv.addColumn(cf_idx,pkinfo_alliancePk,Bytes.toBytes(new_probe_APack));
   				   	 			 	puts_probe_search2.add(NewProbe_Search2_kv);
   				   	 			 	//----end for search2
   				   	 			 	//Start search3 updates using new key from probe: cmn cf
   				   	 			 	String newSearch3Key = new_probe_upd.concat(new_probe_vin).concat(new_probe_ts);
   				   	 			 	Put NewProbe_Search3_kv = new Put(Bytes.toBytes(newSearch3Key));  //VIN+TIMESTAMP
   				   	 			 	NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(new_probe_vin));
   				   	 			 	NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_Timestamp,Bytes.toBytes(new_probe_ts));
   				   	 			 	NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_probe_upd));
   				   	 			 	//NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_alliancePk,Bytes.toBytes(new_probe_APack));
   				   	 			 	puts_probe_search3.add(NewProbe_Search3_kv);
   				   	 			 	//----end for search3
   				   	 			 	
   				   	 			 	//collect old keys for search1, search2, search3 for deletion
   				   	 			 	String old_probe_vin = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_orignalKey));
   				   	 			 	String old_probe_APack = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_alliancePk));
   				   	 			 	String old_probe_upd = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_upd));
   				   	 			 	String old_probe_ts = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_Timestamp));
   				   	 			 	//Search1:ap+upd+ts
   				   	 			 	String oldSearch1Key = old_probe_APack.concat(old_probe_upd).concat(old_probe_ts);
   				   	 			 	Delete d_old_search1 = new Delete(Bytes.toBytes(oldSearch1Key));
   				   	 			    delete_probe_search1.add(d_old_search1);
   				   	 			    //Search2:upd+ap+ts
   				   	 			    String oldSearch2Key = old_probe_upd.concat(old_probe_APack).concat(old_probe_ts);
   				  	 			 	Delete d_old_search2 = new Delete(Bytes.toBytes(oldSearch2Key));
   				   	 			    delete_probe_search2.add(d_old_search2);
   				   	 			    //Search3:upd+vin+ts
   				   	 			    String oldSearch3Key = old_probe_upd.concat(old_probe_vin).concat(old_probe_ts);
   				  	 			 	Delete d_old_search3 = new Delete(Bytes.toBytes(oldSearch3Key));
   				   	 			    delete_probe_search3.add(d_old_search3);
   				   	 		
   				   	 			 }
   		   		     //put new record , enrichment
   				   	 //Table blms_probe_tbl = hbaseConnect(blms_probe, conn);
   				  	 TableName blms_probe_tbl_out = TableName.valueOf(hbaseNameSpace+":"+blms_probe);
   					 Table blms_probe_tbl_outConn =  conn.getTable(blms_probe_tbl_out);
   				   	 putInTable(blms_probe_tbl_outConn,puts_probe);
   				   	 blms_probe_tbl_outConn.close();
   				   	 LOGGER.log(Level.INFO,"PROBE_TBL-PROBE KEY ENRICHED..: "+ProbeKey);
   				   	 LOGGER.log(Level.INFO,"Starting Index Tbls updates for PROBE KEY: "+ProbeKey);
   				   	 
   				  	 TableName blms_probe_search1_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_probe_search1);
   					 Table blms_probe_search1_tblConn =  conn.getTable(blms_probe_search1_tbl);
   				   	 putInTable(blms_probe_search1_tblConn,puts_probe_search1);
    				 //delete old record,after Enrichment, update the idx table for new mapping
   				   	 deleteFromTable(blms_probe_search1_tblConn,delete_probe_search1);
   				   	 blms_probe_search1_tblConn.close();
   				   	 LOGGER.log(Level.INFO,"UPDATE IDX:newSearch1Key: "+newSearch1Key);
   				   	 LOGGER.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch1Key);
   				   	 //Table blms_probe_search1_tbl = hbaseConnect(blms_probe_search1, conn);
   				     //putInTable(blms_probe_search1_tbl,puts_probe_search1);
   				     
   				  	 TableName blms_probe_search2_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_probe_search2);
   					 Table blms_probe_search2_tblConn =  conn.getTable(blms_probe_search2_tbl);
   				   	 putInTable(blms_probe_search2_tblConn,puts_probe_search2);
   				   	 deleteFromTable(blms_probe_search2_tblConn,delete_probe_search2);
   				   	 blms_probe_search2_tblConn.close();
   				   	 LOGGER.log(Level.INFO,"UPDATE IDX:newSearch2Key: "+newSearch2Key);
   				   	 LOGGER.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch2Key);
   				   	 //Table blms_probe_search2_tbl = hbaseConnect(blms_probe_search2, conn);
   				   	 //putInTable(blms_probe_search2_tbl,puts_probe_search2);
   				   	 
   				  	 TableName blms_probe_search3_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_probe_search3);
   					 Table blms_probe_search3_tblConn =  conn.getTable(blms_probe_search3_tbl);
   				   	 putInTable(blms_probe_search3_tblConn,puts_probe_search3);
   				   	 deleteFromTable(blms_probe_search3_tblConn,delete_probe_search3);
   				   	 blms_probe_search3_tblConn.close();
   				   	 LOGGER.log(Level.INFO,"UPDATE IDX:newSearch3Key: "+newSearch3Key);
   				   	 LOGGER.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch3Key);
   				   	 //Table blms_probe_search3_tbl = hbaseConnect(blms_probe_search3, conn);
   				   	 //putInTable(blms_probe_search3_tbl,puts_probe_search3);
   				   
   				   	//delete old record,after Enrichment, update the idx table for new mapping
 	
   				   	 //end blms_probe Enrichment
   				   	 
   				   	LOGGER.log(Level.INFO,"BlMS_PROBE Enrichment finished for all matched Probe keys with PVLU Events..");
   				   	
   				   	JOB_DETAILS.put(line_cnt,"success");
	   	 			LOGGER.log(Level.INFO,"Set: line_cnt,\"success\"");
   				   //Put all successfully processed PVLU into "Cash_event_Processed_table here.."
	   	 		 }else if(((!vinIdxcond1_pkinfo_nissanPack.isEmpty()) && vinIdxcond1_pkinfo_nissanPack !=null) ||
			 				((!vinIdxcond2_alliancePk.isEmpty()) && vinIdxcond2_alliancePk !=null))
			 				{
			 				  LOGGER.log(Level.INFO,"ERROR CASE: Arrived Vin is already attached to another Nissan-Pack: "+vinIdxcond1_pkinfo_nissanPack);
			 				  LOGGER.log(Level.INFO,"ERROR CASE: Arrived Vin is already attached to another Alliance-Pack: "+vinIdxcond2_alliancePk);
			 				  JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived Vin is already attached to another Alliance-Pack:"+vinIdxcond2_alliancePk);
			 				}//end Arrived Vin is not attached to any Pack.
   			 		}else{//end vin_idx_tbl ==null?
   			 			 LOGGER.log(Level.INFO,"ERROR: Table Mapping Issue, Inconsistent Index with Pack Info tbl for VIN IDX..");
   	   			 	     throw new IOException("ERROR: Table Mapping Issue, Inconsistent Index with Pack Info tbl for VIN IDX..");
   			 			} 
 		
  	 		}else { //end arrived pack not attached to any vin
  	 					LOGGER.log(Level.INFO,"Error Case:Attaching arrived pack is already attached/has value for vin.."+cond2_pkinfo_vin);
   			 			JOB_DETAILS.put(line_cnt,"Error Case:Attaching arrived pack is already attached/has value for vin.."+cond2_pkinfo_vin);
  	 			}
	    }else { //end arrived attaching AP not has blmsPack
	    	LOGGER.log(Level.INFO,"Error Case:Pack_blms_id column in pack_info_tbl is empty.."+token[1]);
	    	JOB_DETAILS.put(line_cnt,"Error Case:Pack_blms_id column in pack_info_tbl is empty.."+token[1]);
			}
   	 }else { //end Packinfo result ==null?
   		 	LOGGER.log(Level.INFO,"Error Case:Key not found for this attaching Pack in PackInfo.."+token[1]);
	    	JOB_DETAILS.put(line_cnt,"Error Case:Key not found for this attaching Pack in PackInfo.."+token[1]);
   	 		}
		return JOB_DETAILS;
 }//end setApply()


public static void putInTable(Table PutIntoTable, Put PutRowList) {
 	try {
 		PutIntoTable.put(PutRowList);
 		PutIntoTable.close();
   	 	  }catch (RetriesExhaustedWithDetailsException e) { 
				int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
				LOGGER.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
				for (int n = 0; n < numErrors; n++) { 
					LOGGER.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
					//System.out.println("Cause[" + n + "]: " + e.getCause(n));
					LOGGER.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
					//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
					LOGGER.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
					//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
					} 
				LOGGER.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
				LOGGER.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
		}catch(Exception e){
			LOGGER.log(Level.SEVERE,e.toString(),e);
			}finally {
				//PutIntoTable.close();
			}

}//end putTable()

public static void putInTable(Table PutIntoTable, List<Put> PutRowList) {
 	try {
 		PutIntoTable.put(PutRowList);
 		PutIntoTable.close();
   	 	  }catch (RetriesExhaustedWithDetailsException e) { 
				int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
				LOGGER.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
				for (int n = 0; n < numErrors; n++) { 
					LOGGER.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
					//System.out.println("Cause[" + n + "]: " + e.getCause(n));
					LOGGER.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
					//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
					LOGGER.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
					//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
					} 
				LOGGER.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
				LOGGER.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
		}catch(Exception e){
			LOGGER.log(Level.SEVERE,e.toString(),e);
			}
}//end putTable()
public static void deleteFromTable(Table DeleteFromTable, List<Delete> DeleteRowList) {
 	 	try {
 	 		DeleteFromTable.delete(DeleteRowList);
 	 		DeleteFromTable.close();
   	 	  }catch (RetriesExhaustedWithDetailsException e) { 
				int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
				LOGGER.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
				for (int n = 0; n < numErrors; n++) { 
					LOGGER.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
					//System.out.println("Cause[" + n + "]: " + e.getCause(n));
					LOGGER.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
					//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
					LOGGER.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
					//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
					} 
				LOGGER.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
				LOGGER.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
		}catch(Exception e){
			LOGGER.log(Level.SEVERE,e.toString(),e);
			}
}//end deleteFromTable

public static void applyPvlu(Path filePath, Path errCSVfilePath, Connection conn) throws Exception {

/*//FSDataInputStream inputStream = readPvluInput(filePath);
Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://hahdfsqa/user/asp20571bdp");
conf.set("hadoop.security.authentication", "kerberos");
UserGroupInformation.setConfiguration(conf);
// Subject is taken from current user context
UserGroupInformation.loginUserFromSubject(null);
*/
Configuration conf = getHDFSConf();
FileSystem fs = FileSystem.get(conf);
//Path filePath = new Path("/user/asp20571bdp/java_store_test_file");
LOGGER.log(Level.INFO,"HDFS file path for pvlu input:-"+filePath);   
FSDataInputStream inputStream = fs.open(filePath);
LOGGER.log(Level.INFO,"Number of bytes in input:-"+inputStream.available());   
 try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream)))
 	{
	 String line;
	 while ((line = br.readLine()) != null) 
	  {
	  line_cnt++;
	  LOGGER.log(Level.INFO,"input lineCnt taken for processing: "+line_cnt);
	  String token[] =  line.split("\t");
	  LOGGER.log(Level.INFO,"token lenght:-"+token.length);
	   //if(token != null && token.length == 5)
	  if(token != null)
	   {
	        	    				   //for(String value: token)
	        	    		           //{
	        	    		              //System.out.println("token value : "+value);
	        	    		              /* token[0] <- event id: "Pack_Vehicle_Linkage_Update"
	        	    		               * token[1] <- alliancePack
	        	    		               * token[2] <- Battery Setting Date
	        	    		               * token[3] <- Flag
	        	    		               * token[4] <- vin
	        	    		               */
		LOGGER.log(Level.INFO,"Input Event Record for Processing: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]);
	    if(token[3].equals("1")) 
	    {
	    	LOGGER.log(Level.INFO,"Removing pack:-"+token[1]+" vin:-"+token[4]);
	    	LOGGER.log(Level.INFO,"Calling removeApply() ");
	      try {
	        	removeApply(token,conn);
	        	}catch(Exception e) {LOGGER.log(Level.SEVERE,"Cought in removeApply().."); 
	        						 LOGGER.log(Level.SEVERE,e.toString(),e);}
	        	    	    //read pack info and process remove record as per cases logic
	        	    	    //LOGIC TO CHECK :
	        	    		// 1. REMOVING PACK IS PRESENT
	        	    		// 2. BLMS_ID IS PRESENT
	        	    		// 3. THIS VIN IS REALLY ATTACHED TO THIS PACK 
	        	    		// IF ALL ABOVE TRUE , THEN REMOVE THIS VIN FROM THIS PACK by CREATING NEW RECORD AND ADD "REMOVE_DATE" IN NEW RECORD
	    }else if(token[3].equals("2"))
	        {
	    	LOGGER.log(Level.INFO,"Attaching pack:-"+token[1]+" vin:-"+token[4]);
	    	LOGGER.log(Level.INFO,"Calling setApply()");
	        try {
	             setApply(token,conn);
	            }catch(Exception e){//
	            	LOGGER.log(Level.SEVERE,"Cought in setApply().."); 
	        	    LOGGER.log(Level.SEVERE,e.toString(),e);
	        	 }
	             		  //read pack info and process set record as per cases logic
	        	}//end set
	        	    		
	   }//end if token ==null
	  
	}//buffered end
  br.close();
  }catch(Exception e) {	LOGGER.log(Level.SEVERE,"Error In Reading the HDFS input File for PVLU Event Processing");
  						//e.printStackTrace();
  						LOGGER.log(Level.SEVERE,e.toString(),e);
  		}finally {
  	     inputStream.close();
  		 fs.close();
  		}
 
 //@@@End of Remove & Set Event Processing@@@@@@@@ 
 //###Create ErrorCsv File####
 ArrayList<Integer> successKey = new ArrayList<Integer>(); 
 Map<Integer, String> errorKeyVal = new HashMap<Integer, String>();
 //Set<Integer> JD_KeySet = JOB_DETAILS.keySet();
 for (Entry<Integer, String> entry : JOB_DETAILS.entrySet()) {
      Integer key = entry.getKey();
	  String value = entry.getValue();
	  if(value.compareTo("success")==0)
	    {
	    	successKey.add(key);  //Success line number
	    	//call putIntoCashProcessed 
	    }else {
	    	errorKeyVal.put(key, value); // only error line numbers & error reason 
	    	//Create hdfs error csv file for this job to add into JobDetail table.  
		    }
	}
	       
	Integer JD_pvluLine = 0;
	LOGGER.log(Level.INFO,"HDFS Error file path to generate Error CSV:-"+errCSVfilePath); 
	try {
	FileSystem hdfsOut = FileSystem.get(conf);
	//FSDataOutputStream outPutStream = fs.create(errCSVfilePath, true);
	FSDataOutputStream outPutStream = hdfsOut.create(errCSVfilePath, true);
	
	FileSystem hdfsInProcessedPvlu = FileSystem.get(conf);
	FSDataInputStream inputStreamForCashProcessed = hdfsInProcessedPvlu.open(filePath);
	//FSDataInputStream inputStreamForCashProcessed = fs.open(filePath);
	
	LOGGER.log(Level.INFO,"Number of bytes in input:-"+inputStreamForCashProcessed.available());
	
	try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamForCashProcessed)))
	  {
	   String line;
	   while ((line = br.readLine()) != null) 
	    	  {
	      	  JD_pvluLine++;
	      	  LOGGER.log(Level.INFO,"input JD_pvluLine taken for processing: "+JD_pvluLine);
		      String token[] =  line.split("\t");
		      LOGGER.log(Level.INFO,"token lenght:-"+token.length);
		      LOGGER.log(Level.INFO,"Input Event Record For PostProcess: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]);
		      if(successKey.contains(JD_pvluLine))
		         {  LOGGER.log(Level.INFO,"CachProcessed tbl Eligible: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]);
		          	putIntoCashProcessed(token);
		          }else {
		        	  LOGGER.log(Level.INFO,"CachProcessed tbl Not-Eligible: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" "+JOB_DETAILS.get(JD_pvluLine));
		       	    	//String JDline = line+"\t"+JOB_DETAILS.get(JD_pvluLine);
		       	    	//outPutStream.writeUTF(JDline);
		       	    	//outPutStream.writeUTF("\n");
		       	    	WriteToCsvErrFile(JD_pvluLine, line, errCSVfilePath, outPutStream);
		       	    	//errCSVfilePath 
		       	    	}
		      }
	    br.close();
		}catch(Exception e) {
				LOGGER.log(Level.SEVERE,"Error In Reading file for Cash Processed Processing...");
				LOGGER.log(Level.SEVERE,e.toString(),e);
				
			}finally {
				outPutStream.close();
				inputStreamForCashProcessed.close();
	//			inputStream.close();
				hdfsOut.close();
				hdfsInProcessedPvlu.close();
	//			fs.close();
			}
	}catch(IOException e)
	{e.printStackTrace();}
	
	LOGGER.log(Level.INFO,"JobDetail Log successfully created on HDFS : "+errCSVfilePath);
}
		
private static Configuration getHDFSConf() throws IOException {
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://hahdfsqa/user/asp20571bdp");
	conf.set("hadoop.security.authentication", "kerberos");
	UserGroupInformation.setConfiguration(conf);
	// Subject is taken from current user context
	UserGroupInformation.loginUserFromSubject(null);
	return conf;
}

private static Configuration getHBASEConf() throws IOException {
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
	return configuration;
}


private static void WriteToCsvErrFile(Integer jD_pvluLine, String line, Path errCSVfilePath, FSDataOutputStream outPutStream) throws IOException {
	//Create error CSV for JOB_DETAIL table
	//String JDline = String.join("\t",line,JOB_DETAILS.get(jD_pvluLine));
	String JDline = line+"\t"+JOB_DETAILS.get(jD_pvluLine);
	outPutStream.writeUTF(JDline);
	outPutStream.writeUTF("\n");
	outPutStream.flush();
}
public static void putIntoCashProcessed(String[] token) throws IOException {
	 //Table blms_cache_processed_tbl = hbaseConnect(blms_cache_processed, conn);
	 //put all successfully processed pvlu input into table
	 String newCPKey = new_UPD.concat(token[1]).concat(token[4]);
	 Put NewCashProcessed_kv = new Put(Bytes.toBytes(newCPKey));  //token[1] <-AP;token[4] <- vin
	 NewCashProcessed_kv.addColumn(cf_cp,pkinfo_event_id,cp_event_id);
	 NewCashProcessed_kv.addColumn(cf_cp,pkinfo_orignalKey,Bytes.toBytes(token[1]));
	 NewCashProcessed_kv.addColumn(cf_cp,pkinfo_Timestamp,Bytes.toBytes(token[2])); //token[2] <- battery Setting Date
	 NewCashProcessed_kv.addColumn(cf_cp,cp_VFReadDate,Bytes.toBytes(V_READ_DATE));
	 NewCashProcessed_kv.addColumn(cf_cp,pkinfo_upd,Bytes.toBytes(new_UPD));
	 NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_001,Bytes.toBytes(token[3]));
	 NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_002,Bytes.toBytes(token[4]));
	 
	 TableName blms_cache_processed_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_cache_processed);
	 Table blms_cache_processed_tblConn =  conn.getTable(blms_cache_processed_tbl);
	 putInTable(blms_cache_processed_tblConn,NewCashProcessed_kv);
	 blms_cache_processed_tblConn.close();
	 LOGGER.log(Level.INFO,"newCPKey Added : "+newCPKey);
	 //putInTable(blms_cache_processed_tbl,NewCashProcessed_kv);
		 /*  	 
		   	n	byte array	V_EVENT_ID
		   	k	byte array	V_ORIGINAL_KEY
		   	l	byte array	V_DATE_TIME
		   	q	byte array	V_READ_DATE
		   	g	byte array	T_UPD_DATE
		   	3	byte array	V_COL_001
		   	4	byte array	V_COL_002
*/
}

public static void loginit() throws SecurityException, IOException {
    fh = new FileHandler("pvlu.log");  
 	LOGGER.addHandler(fh);
 	SimpleFormatter formatter = new SimpleFormatter();  
 	fh.setFormatter(formatter); 
 	LOGGER.setUseParentHandlers(false); 
}
	
public static void main(String args[]) throws Exception {
	if(args.length !=3) {
		throw new IOException("PVLU JAVA process Requires[filePath, newUPD, ReadDate], number of passed args not matching with expected..."); 
	}
	  
	PVLU_OLD.loginit();
	
	Path filePath = new Path(args[0]);
	Path ErrCSVfilePath = new Path(args[0]+".error");
	LOGGER.log(Level.INFO,"Log file initiated for the day:- "+new_UPD);
	LOGGER.log(Level.INFO,"Input pvlu file path: "+filePath);
	LOGGER.log(Level.INFO,"Input pvlu Error CSV file path: "+ErrCSVfilePath);
	LOGGER.log(Level.INFO,"Input pvlu new upd: "+args[1]);
	LOGGER.log(Level.INFO,"Input pvlu new v_read_date: "+args[2]);
	PVLU_OLD.new_UPD=args[1];
	PVLU_OLD.V_READ_DATE=args[2];
	//PVLU.wf_id=""

    //ExecutorService pool;
	//conn = ConnectionFactory.createConnection(conf, pool);//createConnection(configuration); 
    ExecutorService executor = Executors.newFixedThreadPool(1);
    conn = ConnectionFactory.createConnection(getHBASEConf(),executor);
	 
	PVLU_OLD.applyPvlu(filePath,ErrCSVfilePath,conn);
	conn.close();
	 
		}  
    }


