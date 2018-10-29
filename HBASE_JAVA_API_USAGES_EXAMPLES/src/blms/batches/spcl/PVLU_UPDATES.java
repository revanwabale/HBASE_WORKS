/*
 * compile with
 * javac -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PVLU_UPDATES.java
 * nohup java -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PVLU_UPDATES 'hdfs://hahdfsqa/user/asp20571bdp/BLMB0102/PMLU/input/111111_20200201130000/VALID_PMLU_INPUT_FOR_JAVA/part-v009-o000-r-00000' '2020-02-01 13:00:00.000' '20200201130000' > PMLU.log 2>&1 &
 * 
 */
package blms.batches.spcl;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
public class PVLU_UPDATES {
	private static Properties configProp = new Properties();
	static Map<byte[], byte[]> PACK_INFO_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> BLMS_PROBE_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_UPD_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_MAP_TBL = new HashMap<byte[], byte[]>();
	static Map<Integer, String> JOB_DETAILS = new HashMap<Integer, String>(); //Collect the success or error case of each pvlu record
	static int line_cnt = 0;

	private static Path filePath , jdCSVFilePath ;
	private static FileHandler fh_pvlu;
	//private static String wf_id = "0000";
	private static String event_set_flag, new_UPD, V_READ_DATE, PkInfo_VAL_s, PkInfo_VAL_t, PkInfo_VAL_v, PkInfo_VAL_w, PkInfo_VAL_x, PkInfo_VAL_h, PkInfo_VAL_i, PkInfo_VAL_j;
	private static String ProbeKey , newSearch1Key , oldSearch1Key , newSearch2Key , oldSearch2Key , newSearch3Key , oldSearch3Key , pvlu_event_id , event_remove_flag ;
	private  static String defaultFS, hbaseZookeeperPort, hbaseZookeeperQuorum1, hbaseZnodeParent, hbaseMasterPort, hbaseMaster, hbaseRpcTimeout, hbaseClientScannerTimeout, hbaseCellsScannedPerHeartBeatCheck, hbaseClientOperationTimeout, hadoopSecurityAuthentication;
	private  static String PVLU_UPDATES_LOG, hbaseNameSpace, blms_probe, blms_probe_search1, blms_probe_search2, blms_probe_search3, blms_pack_info, blms_pack_info_upd_idx, blms_pack_info_vin_idx, blms_cache_processed;

	private  static byte[] cf_pk, cf_idx, cf_cmn,  pkinfo_rmvdate, pkinfo_PackDateKey, pkinfo_Timestamp, pkinfo_event_id, pkinfo_blms_id, pkinfo_vin, pkinfo_upd, pkinfo_orignalKey, pkinfo_alliancePk, pkinfo_pvdate, pkinfo_nissanPack,  PkInfo_FLD_q, PkInfo_FLD_r, PkInfo_FLD_s, PkInfo_FLD_t, PkInfo_FLD_u, PkInfo_FLD_v, PkInfo_FLD_w, PkInfo_FLD_x, PkInfo_FLD_h, PkInfo_FLD_i, PkInfo_FLD_j;
	//pkinfo_rmvdate wil be always 100 since this is a column taken from reserve
	private  static byte[] CP_V_COL_001, CP_V_COL_002, cf_cp;

	private static Logger LOGGER_EVENT;

	private  static Table blms_pack_info_tblConn, blms_mc_tblConn, blms_pack_info_vin_idx_tblConn, TblBlmsConn, blms_probe_tblConn;
	private  static ResultScanner mc_scanner, scanner,vin_scanner, ProbeScanner, vinIdxScanner;
	private  static FileSystem fs, hdfsOut;
	private  static FSDataOutputStream outPutStream;
	private  static Configuration conf;

	private  static BufferedReader br;
	static Connection conn;

	private static final String PropertyFileName = "/home/asp20571bdp/DEV/BDP_BATCHES/COMMON_UTILS/PROPERTIES/conf_blmb0102.properties";

	static {

		try {
			//InputStream in = new FileInputStream("../../../COMMON_UTILS/PROPERTIES/conf_blmb0102.properties");
			InputStream in = new FileInputStream(PropertyFileName);
			configProp.load(in);
			defaultFS = configProp.getProperty("defaultFS");
			hbaseZookeeperPort = configProp.getProperty("hbaseZookeeperPort");
			hbaseZookeeperQuorum1 = configProp.getProperty("hbaseZookeeperQuorum1");
			hbaseZnodeParent = configProp.getProperty("hbaseZnodeParent");
			hbaseMasterPort = configProp.getProperty("hbaseMasterPort");
			hbaseMaster = configProp.getProperty("hbaseMaster");
			hbaseRpcTimeout = configProp.getProperty("hbaseRpcTimeout");
			hbaseClientScannerTimeout = configProp.getProperty("hbaseClientScannerTimeout");
			hbaseCellsScannedPerHeartBeatCheck = configProp.getProperty("hbaseCellsScannedPerHeartBeatCheck");
			hbaseClientOperationTimeout = configProp.getProperty("hbaseClientOperationTimeout");
			hadoopSecurityAuthentication = configProp.getProperty("hadoopSecurityAuthentication");
			PVLU_UPDATES_LOG = configProp.getProperty("PVLU_UPDATES_LOG");
			pvlu_event_id = configProp.getProperty("pvlu_event_id");
			event_remove_flag = configProp.getProperty("event_remove_flag");
			event_set_flag = configProp.getProperty("event_set_flag");
			cf_pk = Bytes.toBytes(configProp.getProperty("cf_pk"));
			cf_idx = Bytes.toBytes(configProp.getProperty("cf_idx"));
			cf_cmn = Bytes.toBytes(configProp.getProperty("cf_cmn"));
			cf_cp = Bytes.toBytes(configProp.getProperty("cf_cp"));
			pkinfo_blms_id = Bytes.toBytes(configProp.getProperty("pkinfo_blms_id"));
			pkinfo_vin = Bytes.toBytes(configProp.getProperty("pkinfo_vin"));
			pkinfo_upd = Bytes.toBytes(configProp.getProperty("pkinfo_upd"));
			pkinfo_orignalKey = Bytes.toBytes(configProp.getProperty("pkinfo_orignalKey"));
			pkinfo_alliancePk = Bytes.toBytes(configProp.getProperty("pkinfo_alliancePk"));
			pkinfo_pvdate = Bytes.toBytes(configProp.getProperty("pkinfo_pvdate"));
			pkinfo_nissanPack = Bytes.toBytes(configProp.getProperty("pkinfo_nissanPack"));
			pkinfo_rmvdate = Bytes.toBytes(configProp.getProperty("pkinfo_rmvdate"));
			pkinfo_PackDateKey = Bytes.toBytes(configProp.getProperty("pkinfo_PackDateKey"));
			pkinfo_Timestamp = Bytes.toBytes(configProp.getProperty("pkinfo_Timestamp"));
			pkinfo_event_id = Bytes.toBytes(configProp.getProperty("pkinfo_event_id"));
			CP_V_COL_001 = Bytes.toBytes(configProp.getProperty("CP_V_COL_001"));
			CP_V_COL_002 = Bytes.toBytes(configProp.getProperty("CP_V_COL_002"));
			hbaseNameSpace = configProp.getProperty("hbaseNameSpace");
			blms_probe = configProp.getProperty("blms_probe");
			blms_probe_search1 = configProp.getProperty("blms_probe_search1");
			blms_probe_search2 = configProp.getProperty("blms_probe_search2");
			blms_probe_search3 = configProp.getProperty("blms_probe_search3");
			blms_pack_info = configProp.getProperty("blms_pack_info");
			blms_pack_info_upd_idx = configProp.getProperty("blms_pack_info_upd_idx");
			blms_pack_info_vin_idx = configProp.getProperty("blms_pack_info_vin_idx");
			blms_cache_processed = configProp.getProperty("blms_cache_processed");

			//Addition of AMO Cols:
			PkInfo_VAL_s= configProp.getProperty("PkInfo_VAL_s");
			PkInfo_VAL_t= configProp.getProperty("PkInfo_VAL_t");
			PkInfo_VAL_v= configProp.getProperty("PkInfo_VAL_v");
			PkInfo_VAL_w= configProp.getProperty("PkInfo_VAL_w");
			PkInfo_VAL_x= configProp.getProperty("PkInfo_VAL_x");
			PkInfo_VAL_h= configProp.getProperty("PkInfo_VAL_h");
			PkInfo_VAL_i= configProp.getProperty("PkInfo_VAL_i");
			PkInfo_VAL_j= configProp.getProperty("PkInfo_VAL_j");

			PkInfo_FLD_q= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_q"));
			PkInfo_FLD_r= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_r"));
			PkInfo_FLD_s= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_s"));
			PkInfo_FLD_t= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_t"));
			PkInfo_FLD_u= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_u"));
			PkInfo_FLD_v= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_v"));
			PkInfo_FLD_w= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_w"));
			PkInfo_FLD_x= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_x"));
			PkInfo_FLD_h= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_h"));
			PkInfo_FLD_i= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_i"));
			PkInfo_FLD_j= Bytes.toBytes(configProp.getProperty("PkInfo_FLD_j"));

			LOGGER_EVENT = Logger.getLogger(PVLU_UPDATES.class.getName());
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SEVERE:UnHandled Exception in static intilization block..."+e.getMessage());
			System.exit(1);
		}
	}

	private static void printInitialValues() {

		LOGGER_EVENT.log(Level.INFO,"PropertyFileName: "+PropertyFileName);
		LOGGER_EVENT.log(Level.INFO,"defaultFS: "+defaultFS);
		LOGGER_EVENT.log(Level.INFO,"hbaseZookeeperPort: "+hbaseZookeeperPort);
		LOGGER_EVENT.log(Level.INFO,"hbaseZookeeperQuorum1: "+hbaseZookeeperQuorum1);
		LOGGER_EVENT.log(Level.INFO,"hbaseZnodeParent: "+hbaseZnodeParent);
		LOGGER_EVENT.log(Level.INFO,"hbaseMasterPort: "+hbaseMasterPort);
		LOGGER_EVENT.log(Level.INFO,"hbaseMaster: "+hbaseMaster);
		LOGGER_EVENT.log(Level.INFO,"hbaseRpcTimeout: "+hbaseRpcTimeout);
		LOGGER_EVENT.log(Level.INFO,"hbaseClientScannerTimeout: "+hbaseClientScannerTimeout);
		LOGGER_EVENT.log(Level.INFO,"hbaseCellsScannedPerHeartBeatCheck: "+hbaseCellsScannedPerHeartBeatCheck);
		LOGGER_EVENT.log(Level.INFO,"hbaseClientOperationTimeout: "+hbaseClientOperationTimeout);
		LOGGER_EVENT.log(Level.INFO,"hadoopSecurityAuthentication: "+hadoopSecurityAuthentication);
		LOGGER_EVENT.log(Level.INFO,"PVLU_UPDATES_LOG: "+PVLU_UPDATES_LOG);
		LOGGER_EVENT.log(Level.INFO,"pvlu_event_id: "+pvlu_event_id);
		LOGGER_EVENT.log(Level.INFO,"event_remove_flag: "+event_remove_flag);
		LOGGER_EVENT.log(Level.INFO,"event_set_flag: "+event_set_flag);
		LOGGER_EVENT.log(Level.INFO,"cf_pk: "+Bytes.toString(cf_pk));
		LOGGER_EVENT.log(Level.INFO,"cf_idx: "+Bytes.toString(cf_idx));
		LOGGER_EVENT.log(Level.INFO,"cf_cmn: "+Bytes.toString(cf_cmn));
		LOGGER_EVENT.log(Level.INFO,"cf_cp: "+Bytes.toString(cf_cp));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_blms_id: "+Bytes.toString(pkinfo_blms_id));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_vin: "+Bytes.toString(pkinfo_vin));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_upd: "+Bytes.toString(pkinfo_upd));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_orignalKey: "+Bytes.toString(pkinfo_orignalKey));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_alliancePk: "+Bytes.toString(pkinfo_alliancePk));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_pvdate: "+Bytes.toString(pkinfo_pvdate));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_nissanPack: "+Bytes.toString(pkinfo_nissanPack));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_rmvdate: "+Bytes.toString(pkinfo_rmvdate));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_PackDateKey: "+Bytes.toString(pkinfo_PackDateKey));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_Timestamp: "+Bytes.toString(pkinfo_Timestamp));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_event_id: "+Bytes.toString(pkinfo_event_id));
		LOGGER_EVENT.log(Level.INFO,"CP_V_COL_001: "+Bytes.toString(CP_V_COL_001));
		LOGGER_EVENT.log(Level.INFO,"CP_V_COL_002: "+Bytes.toString(CP_V_COL_002));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_q: "+Bytes.toString(PkInfo_FLD_q));
		LOGGER_EVENT.log(Level.INFO,"hbaseNameSpace: "+hbaseNameSpace);
		LOGGER_EVENT.log(Level.INFO,"blms_probe: "+blms_probe);
		LOGGER_EVENT.log(Level.INFO,"blms_probe_search1: "+blms_probe_search1);
		LOGGER_EVENT.log(Level.INFO,"blms_probe_search2: "+blms_probe_search2);
		LOGGER_EVENT.log(Level.INFO,"blms_probe_search3: "+blms_probe_search3);
		LOGGER_EVENT.log(Level.INFO,"blms_pack_info: "+blms_pack_info);
		LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx: "+blms_pack_info_upd_idx);
		LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx: "+blms_pack_info_vin_idx);
		LOGGER_EVENT.log(Level.INFO,"blms_cache_processed: "+blms_cache_processed);

		LOGGER_EVENT.log(Level.INFO,"AMO COLUMN LIST FROM PROPERTY FILE(Displaying..) ");
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_s: "+PkInfo_VAL_s);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_t: "+PkInfo_VAL_t);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_v: "+PkInfo_VAL_v);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_w: "+PkInfo_VAL_w);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_x: "+PkInfo_VAL_x);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_h: "+PkInfo_VAL_h);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_i: "+PkInfo_VAL_i);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_VAL_j: "+PkInfo_VAL_j);
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_q: "+Bytes.toString(PkInfo_FLD_q));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_r: "+Bytes.toString(PkInfo_FLD_r));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_s: "+Bytes.toString(PkInfo_FLD_s));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_t: "+Bytes.toString(PkInfo_FLD_t));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_u: "+Bytes.toString(PkInfo_FLD_u));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_v: "+Bytes.toString(PkInfo_FLD_v));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_w: "+Bytes.toString(PkInfo_FLD_w));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_x: "+Bytes.toString(PkInfo_FLD_x));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_h: "+Bytes.toString(PkInfo_FLD_h));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_i: "+Bytes.toString(PkInfo_FLD_i));
		LOGGER_EVENT.log(Level.INFO,"PkInfo_FLD_j: "+Bytes.toString(PkInfo_FLD_j));


	}
	
public static Result getLatestRecord(String tableName, String prefixFilterValue) throws IOException {
		
		//List keys = new ArrayList<Result>();
		Result LastRow=null ;
		
		LOGGER_EVENT.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(hbaseNameSpace + ":" + tableName);
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
			LOGGER_EVENT.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(LastRow.getRow()));
		else
			LOGGER_EVENT.log(Level.INFO, "Record not Found....");

		return LastRow;
	}

	public static void pvluRemoveApply(String[] token) throws Exception{
		try {
			LOGGER_EVENT.log(Level.INFO,"Inside Of pvluRemoveApply() for removing AlliancePack: "+token[1]+" & Vin: "+token[4]);
/*			blms_pack_info_tblConn = getTable(blms_pack_info);
			Scan scan = new Scan(); 
			FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[1])));
			scan.setFilter(allFilters);
			scan.setReversed(true); // read the latest available key and values for this AP
			scan.setMaxResultSize(1);
			LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+scan.toString());
			scanner = blms_pack_info_tblConn.getScanner(scan);
			Result result = scanner.next();
	*/		
			Result result = getLatestRecord(blms_pack_info, token[1]);
			if(result != null)
			{  PACK_INFO_TBL = result.getFamilyMap(cf_pk);
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get( pkinfo_orignalKey)));
			String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
			String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
			LOGGER_EVENT.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
			LOGGER_EVENT.log(Level.INFO,"cond2_pkinfo_vin: "+cond2_pkinfo_vin);

			if(cond1_blms_pack_id !=null && !cond1_blms_pack_id.isEmpty()) 	
			{
				LOGGER_EVENT.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
				//blms id present, arrived alliance pack present.: true
				LOGGER_EVENT.log(Level.INFO,"Next check arrived vin really attached to this pack.."+token[4]);
				//token[4] <-Arrived vin
				if((cond2_pkinfo_vin !=null && !cond2_pkinfo_vin.isEmpty()) && cond2_pkinfo_vin.compareTo(token[4])==0){
					//Arrived "VIN" is also attached to arrived Alliance pack
					LOGGER_EVENT.log(Level.INFO,"Arrived Vin is Matching, Arrived VIN: "+token[4]); 
					//PACK_INFO_TBL = result.getFamilyMap(cf_pk);  
					// token[1] <- AP
					// get the "token[1]+pkinfo_upd" values from packinfo latest record 
					// Use PUT on : upd_idx tbl  "new UPD+token[1]"=> b:token[1]; g:new UPD
					// Use PUT on : vin_idx_tbl  "token[4]+new UPD"=> b:token[1]; g:new UPD
					// Create New Records in pack-info: with new key(token[1]+new UPD) and pk:vin=null,pk:pvdate=null,pk:removedate=removeDate
					/*
    	 				b	-> Alliance Pack1 -> null
    	 				g	-> T_UPD_DATE1 -> new_UPD
    	 				f	-> Battery setting Date -> null
    	 				c	-> N_Pack(To check PVLC) -> null
    	 				d	-> VIN -> same VIN
					 */	
					LOGGER_EVENT.log(Level.INFO,"prepare new record for:vin_idx_tbl; Detach VIN.."+cond2_pkinfo_vin);
					//prepare new record for:vin_idx_tbl; Detach VIN
					byte [] newVinIdxKey = Bytes.toBytes(token[4].concat(new_UPD));
					Put NewVinIdx_kv = new Put(newVinIdxKey); //token[4] <-Arrived VIN
					NewVinIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
					NewVinIdx_kv.addColumn(cf_idx,pkinfo_vin,PACK_INFO_TBL.get(pkinfo_vin));
					LOGGER_EVENT.log(Level.INFO,Bytes.toString(pkinfo_upd),new_UPD);
					LOGGER_EVENT.log(Level.INFO,Bytes.toString(pkinfo_vin),Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Started for the event Line Number: "+line_cnt);
					putInTable(blms_pack_info_vin_idx,NewVinIdx_kv);

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Finished for the event Line Number: "+line_cnt);
					//end vin_idx Updated for New record insert
					LOGGER_EVENT.log(Level.INFO,"vin_idx Updated for New record insert.."+Bytes.toString(newVinIdxKey));
					//prepare new record for:upd_idx_tbl
					LOGGER_EVENT.log(Level.INFO,"Prepare new record for:upd_idx_tbl..");

					byte [] newUpdIdxKey = Bytes.toBytes(new_UPD.concat(token[1]));
					Put NewUpdIdx_kv = new Put(newUpdIdxKey);  //token[1] <- AP
					NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
					NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,PACK_INFO_TBL.get(pkinfo_orignalKey));
					LOGGER_EVENT.log(Level.INFO,Bytes.toString(pkinfo_upd),new_UPD);
					LOGGER_EVENT.log(Level.INFO,Bytes.toString(pkinfo_alliancePk),Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Started for the event Line Number: "+line_cnt);
					putInTable(blms_pack_info_upd_idx,NewUpdIdx_kv);

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Finished for the event Line Number: "+line_cnt);
					//end UPD_idx Updated for New record insert
					LOGGER_EVENT.log(Level.INFO,"End UPD_idx Updated for New record insert.."+Bytes.toString(newUpdIdxKey));

					//Preparing new record for packInfo:
					LOGGER_EVENT.log(Level.INFO,"Preparing new record for packInfo..");
					PACK_INFO_TBL.put(pkinfo_vin,null);
					PACK_INFO_TBL.put(pkinfo_pvdate,null);
					PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(new_UPD));
					PACK_INFO_TBL.put(pkinfo_rmvdate,Bytes.toBytes(token[2])); //token[2] <- battery remove date from pvlu rec
					PACK_INFO_TBL.put(PkInfo_FLD_r, Bytes.toBytes(token[10])); //Set inputEventFilename as pmlu event registering, AMO cols
					PACK_INFO_TBL.put(PkInfo_FLD_q, Bytes.toBytes(V_READ_DATE)); //Set ReadDate for this CSV registration, AMO cols
					//PACK_INFO_TBL.entrySet();
					byte [] newPackInfoKey = Bytes.toBytes(token[1].concat(new_UPD));
					Put NewPackInfo_kv = new Put(newPackInfoKey); //token[1] <- AP
					for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
						byte[] key = entry.getKey();
						byte[] value = entry.getValue();
						LOGGER_EVENT.log(Level.INFO,Bytes.toString(key),Bytes.toString(value));
						NewPackInfo_kv.addColumn(cf_pk,key, value);
					}

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Started for the event Line Number: "+line_cnt);
					putInTable(blms_pack_info,NewPackInfo_kv);

					LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Finished for the event Line Number: "+line_cnt);
					//end of new record for packInfo insert 
					LOGGER_EVENT.log(Level.INFO,"End of new record for packInfo insert.."+Bytes.toString(newPackInfoKey));
					JOB_DETAILS.put(line_cnt,"success");
					LOGGER_EVENT.log(Level.INFO,"Set:- JOB_DETAILS:"+String.valueOf(line_cnt)+"=success");

					//@@Insert successful event in cash_processed_tbl 
					LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert Started for the event Line Number: "+line_cnt);
					putIntoCashProcessed(token);
					LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert finished for the event Line Number: "+line_cnt);

				}else {
					LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived VIN is not attached to Arrived Rmoving Pack.."+token[1]);
					JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived VIN is not attached to Arrived Rmoving Pack.."+token[1]);
				}
			}else{
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
			}

			}else {
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Removing Pack is not Present in PackInfo Table.."+token[1]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE: Removing Pack is not Present in PackInfo Table.."+token[1]);
			}
		}catch(Exception e) {
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			throw new Exception("SEVERE:Exception in pvluRemoveApply()....."+e.getMessage());
		}
	}//end pvluRemoveApply()


	public static void pvluSetApply(String[] token) throws Exception{
		try {
			LOGGER_EVENT.log(Level.INFO,"Inside Of pvluSetApply() for attaching AlliancePack: "+token[1]+" & Vin: "+token[4]);
			//1. Check AlliancePack, BlmsPack present for arrived event in "pk_info" tbl.
			//2. if 1 true, then check "vin" feild is empty for this arrived pack in "pk_info" tbl.
			//(arrived pack is not linked to any other vin.)
			//3. if 2 true, then check, Arrived "vin" is not already liked with any pack. (in vin_idx tbl)
			//4. if all above true, then apply the Set to packinfo:
			//4.1: After pk_info, update the : upd_idx, vin_idx_tbl,
			//Conditional (probe:timestamp > pkinfo:pvdate ) - probe:cmn, search1, search2, search3.

			//LOGGER_EVENT.log(Level.INFO,"Inside Of removeApply() for attaching AlliancePack: "+token[1]+" & Vin: ",token[4]);
			//TableName blms_pack_info_tbl = TableName.valueOf(hbaseNameSpace+":"+blms_pack_info);
			//Table blms_pack_info_tblConn =  conn.getTable(blms_pack_info_tbl);
/*			blms_pack_info_tblConn = getTable(blms_pack_info);
			Scan scan = new Scan(); 
			FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[1])));
			scan.setFilter(allFilters);
			scan.setReversed(true); // read the latest available key and values for this AP
			scan.setMaxResultSize(1);
			LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+scan.toString());
			scanner = blms_pack_info_tblConn.getScanner(scan);
			Result result = scanner.next();
*/			
			Result result = getLatestRecord(blms_pack_info, token[1]);
			if(result != null)
			{  PACK_INFO_TBL = result.getFamilyMap(cf_pk);
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
			LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get( pkinfo_orignalKey)));
			String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
			String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
			String cond3_attaching_vin = token[4]; //arrived attachinging VIN
			LOGGER_EVENT.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
			LOGGER_EVENT.log(Level.INFO,"cond2_pkinfo_vin: "+cond2_pkinfo_vin);
			LOGGER_EVENT.log(Level.INFO,"cond3_attaching_vin: "+cond3_attaching_vin);

			if(cond1_blms_pack_id !=null && !cond1_blms_pack_id.isEmpty())	
			{
				LOGGER_EVENT.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
				LOGGER_EVENT.log(Level.INFO,"Next check : vin columns is null or not present for arrived attaching pack in pkinfo tbl: "+token[1]);
				if(cond2_pkinfo_vin ==null || cond2_pkinfo_vin.isEmpty())	
				{
					LOGGER_EVENT.log(Level.INFO,"Arrived Pack is not attached to any other VIN: "+token[1]);
					LOGGER_EVENT.log(Level.INFO,"Next check arrived VIN is not attached to any other Pack: "+token[4]);

/*					blms_pack_info_vin_idx_tblConn = getTable(blms_pack_info_vin_idx);
					Scan vinIdxScan = new Scan(); 
					LOGGER_EVENT.info("Set start Row for scan: "+token[4]); //arrived VIN
					FilterList vinIdxallFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
					vinIdxallFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4])));
					vinIdxScan.setFilter(vinIdxallFilters);
					vinIdxScan.setReversed(true); // read the latest available key and values for this AP
					vinIdxScan.setMaxResultSize(1);
					LOGGER_EVENT.info("scan object to string before execute: "+vinIdxScan.toString());
					vinIdxScanner = blms_pack_info_vin_idx_tblConn.getScanner(vinIdxScan);
					//logic to check if VIN is already attached to anther nissan pack:
					Result VinIdxresult = vinIdxScanner.next(); //retrive latest record for this VIN if present.
*/
					Result VinIdxresult = getLatestRecord(blms_pack_info_vin_idx, token[4]);
					if(VinIdxresult != null)
					{
						VIN_IDX_TBL = VinIdxresult.getFamilyMap(cf_idx);
						LOGGER_EVENT.log(Level.INFO,"latest key value from VIN_IDX TBL for pvlu_vin: "+Bytes.toString(VIN_IDX_TBL.get(pkinfo_vin)));
						LOGGER_EVENT.log(Level.INFO,"latest key value from VIN_IDX TBL for already attached  NissanPack: "+Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack)));
						//Check AP is Attached or Nissanpack attached(pvlc) to this VIN
						String vinIdxcond1_pkinfo_nissanPack= Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack));
						LOGGER_EVENT.log(Level.INFO,"vinIdxcond1_pkinfo_nissanPack: "+vinIdxcond1_pkinfo_nissanPack);
						String vinIdxcond2_alliancePk= Bytes.toString(VIN_IDX_TBL.get(pkinfo_alliancePk));
						LOGGER_EVENT.log(Level.INFO,"vinIdxcond2_alliancePk: "+vinIdxcond2_alliancePk);

						if((vinIdxcond1_pkinfo_nissanPack ==null || cond2_pkinfo_vin.isEmpty()) ||
						   (vinIdxcond2_alliancePk ==null || vinIdxcond2_alliancePk.isEmpty())) {
							LOGGER_EVENT.log(Level.INFO,"Arrived Vin is not attached to another nissan Pack: "+token[4]);
							//1. Update the PACKINFO tbl
							//Preparing new record for packInfo:
							LOGGER_EVENT.log(Level.INFO,"Preparing new record for packInfo..");
							PACK_INFO_TBL.put(pkinfo_vin,Bytes.toBytes(token[4]));  //set the arrived VIN
							PACK_INFO_TBL.put(pkinfo_pvdate,Bytes.toBytes(token[2])); //set the Battery setting Date as pvdate
							PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(new_UPD));   // new UPD date for record
							PACK_INFO_TBL.put(PkInfo_FLD_r, Bytes.toBytes(token[10])); //Set inputEventFilename as pmlu event registering, AMO cols
							PACK_INFO_TBL.put(PkInfo_FLD_q, Bytes.toBytes(V_READ_DATE)); //Set ReadDate for this CSV registration, AMO cols

							byte [] newPackInfoKey = Bytes.toBytes(token[1].concat(new_UPD));
							//PACK_INFO_TBL.put(pkinfo_rmvdate,Bytes.toBytes(token[2])); //token[2] <- battery remove date from pvlu rec
							Put NewPackInfo_kv = new Put(newPackInfoKey); //token[1] <- AP
							for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
								byte[] key = entry.getKey();
								byte[] value = entry.getValue();
								LOGGER_EVENT.log(Level.INFO,Bytes.toString(key),Bytes.toString(value));
								NewPackInfo_kv.addColumn(cf_pk,key, value);
							}

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info,NewPackInfo_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Finished for the event Line Number: "+line_cnt);
							//end of new record for packInfo insert 
							LOGGER_EVENT.log(Level.INFO,"End of new record for packInfo insert:"+Bytes.toString(newPackInfoKey));

							//2. Update the UPD_IDX table
							// Prepare new record for:upd_idx_tbl
							LOGGER_EVENT.log(Level.INFO,"Prepare new record for:upd_idx_tbl: "+token[1]);
							byte [] newUpdKey = Bytes.toBytes(new_UPD.concat(token[1]));
							Put NewUpdIdx_kv = new Put(newUpdKey);  //token[1] <- AP
							NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
							NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,PACK_INFO_TBL.get(pkinfo_orignalKey));

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info_upd_idx,NewUpdIdx_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Finished for the event Line Number: "+line_cnt);
							//end UPD_idx Updated for New record insert
							LOGGER_EVENT.log(Level.INFO,"End UPD_idx Updated for New record insert: "+Bytes.toString(newUpdKey));
							//3. Update the VIN_IDX tbl

							/*  b <- 	Alliance Pack1
   				   	 				g <- 	T_UPD_DATE1
   				   	 				f <-	Battery setting Date
   				   	 				c <-	N_Pack(To check PVLC)
   				   	 			    d <-	VIN  
							 */
							LOGGER_EVENT.log(Level.INFO,"Prepare new record for:vin_idx_tbl; Attach VIN: "+token[4]);
							//Prepare new record for:vin_idx_tbl; 
							byte [] newVinKey = Bytes.toBytes(token[4].concat(new_UPD));
							Put NewVinIdx_kv = new Put(newVinKey); //token[4] <-Arrived VIN
							NewVinIdx_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD)); //new_UPD
							NewVinIdx_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(token[4])); //VIN column
							NewVinIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,Bytes.toBytes(token[1])); //Attached AP
							NewVinIdx_kv.addColumn(cf_idx,pkinfo_pvdate,Bytes.toBytes(token[2])); //Battery Setting Date
							NewVinIdx_kv.addColumn(cf_idx,pkinfo_nissanPack,PACK_INFO_TBL.get(pkinfo_nissanPack)); //NissanPack

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info_vin_idx,NewVinIdx_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Finished for the event Line Number: "+line_cnt);
							//end vin_idx Updated for New record insert
							LOGGER_EVENT.log(Level.INFO,"vin_idx Updated for New record insert: "+Bytes.toString(newVinKey));

							//4. Enrich the Valid Probe & its idx tbls for this VIN-PACK attachment	
							LOGGER_EVENT.log(Level.INFO,"blms_probe enrichment Started for VIN: "+token[4]);

							blms_probe_tblConn = getTable(blms_probe);
							Scan probeScan = new Scan(); 
							LOGGER_EVENT.log(Level.INFO,"Set start Row for scan: "+token[4]+token[2]);
							probeScan.setStartRow(Bytes.toBytes(token[4]+token[2]));
							probeScan.addFamily(cf_cmn);
							probeScan.setBatch(15);
							probeScan.setCaching(1000);
							//pvlu_vin+pvlu_setpvDate <- VIN+PVdate(new attaching pvdate, utc converted)
							FilterList probeAllFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
							probeAllFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4])));
							probeScan.setFilter(probeAllFilters);
							//scan.setReversed(true); // read the latest available key and values for this AP
							//scan.setMaxResultSize(1000000000);
							LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+probeScan.toString());
							ProbeScanner = blms_probe_tblConn.getScanner(probeScan);
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
								//LOGGER_EVENT.info("Check condition for: Probe,search1,search2,search3 updates..");
								// for blms_probe, directly issue put for all rowprifix and start row matched.
								BLMS_PROBE_TBL = ProbeResult.getFamilyMap(cf_cmn);
								String ProbeKey = Bytes.toString(ProbeResult.getRow());
								LOGGER_EVENT.log(Level.INFO,"Probe-RokwKey for Enrichment:  "+ProbeKey);
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

								//LOGGER_EVENT.log(Level.INFO,"Preparing Corresponding Index Tbl Updates For PROBE KEY Started: "+ProbeKey);
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
							
								if(old_probe_APack!=null && !old_probe_APack.isEmpty() && 
								   old_probe_upd!=null && !old_probe_upd.isEmpty() && 
								   old_probe_ts!=null && !old_probe_ts.isEmpty())
								{
								//Search1:ap+upd+ts
								String oldSearch1Key = old_probe_APack.concat(old_probe_upd).concat(old_probe_ts);
								Delete d_old_search1 = new Delete(Bytes.toBytes(oldSearch1Key));
								delete_probe_search1.add(d_old_search1);
								//Search2:upd+ap+ts
								String oldSearch2Key = old_probe_upd.concat(old_probe_APack).concat(old_probe_ts);
								Delete d_old_search2 = new Delete(Bytes.toBytes(oldSearch2Key));
								delete_probe_search2.add(d_old_search2);
								}
								//Search3:upd+vin+ts
								String oldSearch3Key = old_probe_upd.concat(old_probe_vin).concat(old_probe_ts);
								Delete d_old_search3 = new Delete(Bytes.toBytes(oldSearch3Key));
								delete_probe_search3.add(d_old_search3);
								

							}
							//put new record , enrichment
							LOGGER_EVENT.log(Level.INFO,"blms_probe tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_probe,puts_probe);

							LOGGER_EVENT.log(Level.INFO,"blms_probe tbl Insert Finished for the event Line Number: "+line_cnt);
							//LOGGER_EVENT.log(Level.INFO,"PROBE_TBL-PROBE KEY ENRICHED..: "+ProbeKey);
							//LOGGER_EVENT.log(Level.INFO,"Starting Index Tbls updates for PROBE KEY: "+ProbeKey);
							LOGGER_EVENT.log(Level.INFO,"Starting Index Tbls updates for Enriched PROBE KEY Set.. ");

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search1 tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_probe_search1,puts_probe_search1);
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search1 tbl Insert Finished for the event Line Number: "+line_cnt);
							//delete old record,after Enrichment, update the idx table for new mapping
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search1 tbl delete for Old Keys, Started for the event Line Number: "+line_cnt);
							deleteFromTable(blms_probe_search1,delete_probe_search1);

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search1 tbl delete for Old Keys, Finished for the event Line Number: "+line_cnt);
							//LOGGER_EVENT.log(Level.INFO,"UPDATE IDX:newSearch1Key: "+newSearch1Key);
							//LOGGER_EVENT.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch1Key);

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search2 tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_probe_search2,puts_probe_search2);
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search2 tbl Insert Finished for the event Line Number: "+line_cnt);
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search2 tbl old key delete, Started for the event Line Number: "+line_cnt);
							deleteFromTable(blms_probe_search2,delete_probe_search2);

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search2 tbl old key delete, Finished for the event Line Number: "+line_cnt);
							//LOGGER_EVENT.log(Level.INFO,"UPDATE IDX:newSearch2Key: "+newSearch2Key);
							//LOGGER_EVENT.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch2Key);

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search3 tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_probe_search3,puts_probe_search3);
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search3 tbl Insert finished for the event Line Number: "+line_cnt);
							LOGGER_EVENT.log(Level.INFO,"blms_probe_search3 tbl delete for old key, Started for the event Line Number: "+line_cnt);
							deleteFromTable(blms_probe_search3,delete_probe_search3);

							LOGGER_EVENT.log(Level.INFO,"blms_probe_search3 tbl delete for old key, Finished for the event Line Number: "+line_cnt);
							//LOGGER_EVENT.log(Level.INFO,"UPDATE IDX:newSearch3Key: "+newSearch3Key);
							//LOGGER_EVENT.log(Level.INFO,"DELETE IDX:oldSearch1Key: "+oldSearch3Key);

							//@@Insert successful event in cash_processed_tbl 
							LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert Started for the event Line Number: "+line_cnt);
							putIntoCashProcessed(token);
							LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert finished for the event Line Number: "+line_cnt);

							//delete old record,after Enrichment, update the idx table for new mapping
							//end blms_probe Enrichment

							LOGGER_EVENT.log(Level.INFO,"BlMS_PROBE Enrichment finished for all matched Probe keys with PVLU Events..");

							JOB_DETAILS.put(line_cnt,"success");
							LOGGER_EVENT.log(Level.INFO,"Set:- JOB_DETAILS:"+String.valueOf(line_cnt)+"=success");
							//Put all successfully processed PVLU into "Cash_event_Processed_table here.."
						}else if(( vinIdxcond1_pkinfo_nissanPack !=null && (!vinIdxcond1_pkinfo_nissanPack.isEmpty())) ||
								(vinIdxcond2_alliancePk !=null && !vinIdxcond2_alliancePk.isEmpty()))
						{	LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived Vin is already attached to another Nissan-Pack: "+vinIdxcond1_pkinfo_nissanPack);
						LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived Vin is already attached to another Alliance-Pack: "+vinIdxcond2_alliancePk);
						JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived Vin is already attached to another Alliance-Pack:"+vinIdxcond2_alliancePk);
						}//end Arrived Vin is not attached to any Pack.
					}else{//end vin_idx_tbl ==null?
						LOGGER_EVENT.log(Level.INFO,"ERROR: Table Mapping Issue, Inconsistent Index with Pack Info tbl for VIN IDX..");
						JOB_DETAILS.put(line_cnt,"ERROR: Table Mapping Issue, Inconsistent Index with Pack Info tbl for VIN IDX..");
						//throw new IOException("ERROR: Table Mapping Issue, Inconsistent Index with Pack Info tbl for VIN IDX..");
					} 

				}else { //end arrived pack not attached to any vin
					LOGGER_EVENT.log(Level.INFO,"ERROR CASE:Attaching arrived pack is already attached/has value for vin.."+cond2_pkinfo_vin);
					JOB_DETAILS.put(line_cnt,"ERROR CASE:Attaching arrived pack is already attached/has value for vin.."+cond2_pkinfo_vin);
				}
			}else { //end arrived attaching AP not has blmsPack
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE:Pack_blms_id column in pack_info_tbl is empty.."+token[1]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE:Pack_blms_id column in pack_info_tbl is empty.."+token[1]);
			}
			}else { //end Packinfo result ==null?
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE:Key not found for this attaching Pack in PackInfo.."+token[1]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE:Key not found for this attaching Pack in PackInfo.."+token[1]);
			}	}catch(Exception e) {
				LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
				throw new Exception("SEVERE:Exception in pvluSetApply()...."+e.getMessage());
			}
	}//end pvluSetApply()


	public static void putInTable(String blms_tbl, Put PutRowList) throws Exception{
		try {
			//TblBlmsConn = getTable(blms_tbl);
			//TblBlmsConn.put(PutRowList);
			TblBlmsConn = getTable(blms_tbl);
			RowMutations mutations = new RowMutations(PutRowList.getRow());
			mutations.add(PutRowList);
			TblBlmsConn.mutateRow(mutations);
			TblBlmsConn.close();
		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_EVENT.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_EVENT.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				//System.out.println("Cause[" + n + "]: " + e.getCause(n));
				LOGGER_EVENT.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_EVENT.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
				//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
			} 
			LOGGER_EVENT.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_EVENT.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}
	}//end putTable()

	public static void putInTable(String blms_tbl, List<Put> PutRowList) throws Exception {
		try {
			TblBlmsConn = getTable(blms_tbl);
			TblBlmsConn.put(PutRowList);
			TblBlmsConn.close();

			/*		TblBlmsConn = getTable(blms_tbl);
			int i = 0;
			while(i<=PutRowList.size())
			{
			RowMutations mutations = new RowMutations(PutRowList.get(i).getRow());
			mutations.add(PutRowList.get(i));
			TblBlmsConn.mutateRow(mutations);
			i++;
			}
			TblBlmsConn.close(); */

		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_EVENT.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_EVENT.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				//System.out.println("Cause[" + n + "]: " + e.getCause(n));
				LOGGER_EVENT.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_EVENT.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
				//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
			} 
			LOGGER_EVENT.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_EVENT.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}
	}//end putTable()
	public static void deleteFromTable(String DeleteFromTable, List<Delete> DeleteRowList) throws Exception{

		try {
			TblBlmsConn = getTable(DeleteFromTable);
			if(!DeleteFromTable.isEmpty())
			TblBlmsConn.delete(DeleteRowList);
			TblBlmsConn.close();
		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_EVENT.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_EVENT.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				//System.out.println("Cause[" + n + "]: " + e.getCause(n));
				LOGGER_EVENT.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_EVENT.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
				//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
			} 
			LOGGER_EVENT.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_EVENT.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 
			//releaseResources();
			throw new Exception("SEVERE:Exception in deleteFromTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in deleteFromTable()...."+e.getMessage());
		}
	}//end deleteFromTable


	public static void applyUpdates() throws Exception{
		try {	
			conf = getHDFSConf();
			fs = FileSystem.get(conf);
			LOGGER_EVENT.log(Level.INFO,"HDFS file path for pvlu input:-"+filePath);
			if(!fs.exists(filePath)) {
				LOGGER_EVENT.log(Level.SEVERE,"INVALID ARGUMENTS: Path doest not exist..");
				throw new Exception("SEVERE:INVALID ARGUMENTS: Path doest not exist..");
				//LOGGER_EVENT.log(Level.INFO,"INVALID ARGUMENTS: Path doest not exist..");
				//releaseResources();
				//System.exit(1);
			}else {
				LOGGER_EVENT.log(Level.INFO,"Input FilePath Present:"+filePath);
				hdfsOut = FileSystem.get(conf);
				outPutStream = hdfsOut.create(jdCSVFilePath, true);

				FileStatus[] status = fs.listStatus(filePath);
				String line;
				for (int i = 0; i < status.length; i++) {
					LOGGER_EVENT.log(Level.INFO,"subfilepath:"+status[i].getPath().getName());
					br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					LOGGER_EVENT.log(Level.INFO,"In BufferReader..subfilepath:"+status[i].getPath().toString());
					while ((line = br.readLine()) != null) 
					{	
						line_cnt++;
						String token[] =  line.split("\t");
						LOGGER_EVENT.log(Level.INFO,"token lenght:-"+token.length);
						LOGGER_EVENT.log(Level.INFO,"input lineCnt taken for processing: "+line_cnt);
						LOGGER_EVENT.log(Level.INFO,"Input Event Record for Processing: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]);
						if(token[3].equals(event_remove_flag) && token[0].equals(pvlu_event_id))
						{
							LOGGER_EVENT.log(Level.INFO,"Removing vin:-"+token[4]+" Pack:-"+token[1]);
							LOGGER_EVENT.log(Level.INFO,"Calling pvluRemoveApply()");
							pvluRemoveApply(token);
							LOGGER_EVENT.log(Level.INFO,"pvluRemoveApply() Process:Done for Event Line number: "+line_cnt);

						}else if(token[3].equals(event_set_flag) && token[0].equals(pvlu_event_id))
						{
							LOGGER_EVENT.log(Level.INFO,"Attaching vin:-"+token[4]+" Pack:-"+token[1]);
							LOGGER_EVENT.log(Level.INFO,"Calling pvluSetApply()");
							pvluSetApply(token);
							LOGGER_EVENT.log(Level.INFO,"pvluSetApply() Process:Done for Event Line number: "+line_cnt);
						}//end pvlu set

						ProcessErrorEvent(line);
					}//while end
				}//End of file list
			}//end of else file exist
		}catch(Exception e) {	//LOGGER_EVENT.log(Level.SEVERE,"Error In getHDFSConf(), failed to read the hdfs filesystem using provided conf..");
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in applyUpdates()....."+e.getMessage());
		}
		LOGGER_EVENT.log(Level.INFO,"JobDetail Log successfully created on HDFS : "+jdCSVFilePath);
	}
	private static void ProcessErrorEvent(String line) throws Exception{
		String record = JOB_DETAILS.get(line_cnt);
		if (record.compareTo("success")==0){
			LOGGER_EVENT.log(Level.INFO,"SUCCESS, Event Line number: "+line_cnt);
		}else {
			LOGGER_EVENT.log(Level.INFO,"ERROR, Event Line number: "+line_cnt);
		}
		LOGGER_EVENT.log(Level.INFO,"Writing to CSV... ");
		WriteToCsvErrFile(line);
	}

	private static void WriteToCsvErrFile(String line) throws Exception{
		String JDline = line + "\t" + JOB_DETAILS.get(line_cnt) + "\t";
		try {
			outPutStream.writeUTF(JDline);
			outPutStream.writeUTF("\n");
			outPutStream.flush();
			LOGGER_EVENT.log(Level.INFO,"Successfuly inserted into CSV, for Event line number: "+line_cnt);
		} catch (Exception e) {
			//LOGGER_EVENT.log(Level.SEVERE,"Error In WriteToCsvErrFile(), failed to Write into hdfs filesystem.."+jdCSVFilePath);
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in WriteToCsvErrFile()....."+e.getMessage());
		}
	}



	private static Table getTable(String tblName) throws IOException {
		TableName blms_tbl = TableName.valueOf(hbaseNameSpace+":"+tblName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		return blms_tblConn;
	}

	private static Configuration getHDFSConf() throws IOException {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", defaultFS);
		config.set("hadoop.security.authentication", hadoopSecurityAuthentication);
		UserGroupInformation.setConfiguration(config);
		// Subject is taken from current user context
		UserGroupInformation.loginUserFromSubject(null);
		return config;
	}

	private static Configuration getHBASEConf() throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);
		configuration.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum1);
		configuration.set("zookeeper.znode.parent", hbaseZnodeParent);
		configuration.set("hbase.master.port", hbaseMasterPort);
		configuration.set("hbase.master", hbaseMaster);
		configuration.set("hbase.rpc.timeout", hbaseRpcTimeout);
		configuration.set("hbase.client.scanner.timeout.period", hbaseClientScannerTimeout);
		configuration.set("hbase.cells.scanned.per.heartbeat.check", hbaseCellsScannedPerHeartBeatCheck);
		configuration.set("hbase.client.operation.timeout", hbaseClientOperationTimeout);
		return configuration;
	}

	public static void putIntoCashProcessed(String[] token) throws Exception{
		//put all successfully processed pvlu input into table
		String newCPKey = new_UPD.concat(token[1]).concat(token[4]);
		Put NewCashProcessed_kv = new Put(Bytes.toBytes(newCPKey));  //token[1] <-AP;token[4] <- vin
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_event_id,Bytes.toBytes(token[0])); //token[0]: event_type.
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_orignalKey,Bytes.toBytes(token[1]));
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_Timestamp,Bytes.toBytes(token[2])); //token[2] <- battery Setting Date
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_upd,Bytes.toBytes(new_UPD));
		NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_001,Bytes.toBytes(token[3]));
		NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_002,Bytes.toBytes(token[4]));

		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_r,  Bytes.toBytes(token[10]));    //Set inputEventFilename as pmlu event registering
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_q,  Bytes.toBytes(V_READ_DATE));  //Set ReadDate for this CSV registration.

		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_s,  Bytes.toBytes(PkInfo_VAL_s)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_t,  Bytes.toBytes(PkInfo_VAL_t)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_u,  Bytes.toBytes(new_UPD)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_v,  Bytes.toBytes(PkInfo_VAL_v)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_w,  Bytes.toBytes(PkInfo_VAL_w)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_x,  Bytes.toBytes(PkInfo_VAL_x)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_h,  Bytes.toBytes(PkInfo_VAL_h)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_i,  Bytes.toBytes(PkInfo_VAL_i)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_j,  Bytes.toBytes(PkInfo_VAL_j)); 

		putInTable(blms_cache_processed,NewCashProcessed_kv);
		LOGGER_EVENT.log(Level.INFO,"newCPKey Added : "+newCPKey);
	}

	public final static void getResources(String WF_ID) throws SecurityException, IOException {
		SimpleFormatter formatter = new SimpleFormatter();
		//Handler for PMLU 	
		//fh_pvlu = new FileHandler(PVLU_UPDATES_LOG+"_" + new_UPD.replaceAll("[^0-9]", ""));
		fh_pvlu = new FileHandler(PVLU_UPDATES_LOG+"_" + WF_ID.replaceAll("[^0-9]", ""));
		fh_pvlu.setFormatter(formatter);
		LOGGER_EVENT.addHandler(fh_pvlu);
		LOGGER_EVENT.setUseParentHandlers(false);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		conn = ConnectionFactory.createConnection(getHBASEConf(),executor);
	}

	private static void releaseResources() throws Exception{
		try {
			if(ProbeScanner!=null)
			{
				ProbeScanner.close();
				LOGGER_EVENT.log(Level.INFO,"ProbeScanner.close() executed...");
			}
			if(mc_scanner!=null)
			{
				mc_scanner.close();
				LOGGER_EVENT.log(Level.INFO,"mc_scanner.close() executed...");
			}
			if(scanner!=null)
			{
				scanner.close();
				LOGGER_EVENT.log(Level.INFO,"scanner.close() executed...");
			}
			if(vinIdxScanner !=null)
			{
				vinIdxScanner.close();
				LOGGER_EVENT.log(Level.INFO,"vinIdxScanner.close() executed...");
			}
			if(vin_scanner!=null)
			{
				vin_scanner.close();
				LOGGER_EVENT.log(Level.INFO,"vin_scanner.close() executed...");
			}

			if(blms_pack_info_tblConn!=null)
			{
				blms_pack_info_tblConn.close();
				LOGGER_EVENT.log(Level.INFO,"blms_pack_info_tblConn.close() executed...");
			}
			if(blms_mc_tblConn!=null)
			{
				blms_mc_tblConn.close();
				LOGGER_EVENT.log(Level.INFO,"blms_mc_tblConn.close() executed...");
			}
			if(TblBlmsConn!=null)
			{
				TblBlmsConn.close();
				LOGGER_EVENT.log(Level.INFO,"TblBlmsConn.close() executed...");
			}
			if(blms_pack_info_vin_idx_tblConn!=null)
			{
				blms_pack_info_vin_idx_tblConn.close();
				LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx_tblConn.close() executed...");
			}
			if(blms_probe_tblConn!=null)
			{
				blms_probe_tblConn.close();
				LOGGER_EVENT.log(Level.INFO,"blms_probe_tblConn.close() executed...");
			}

			if(br!=null)
			{
				br.close();
				LOGGER_EVENT.log(Level.INFO,"br.close() executed...");
			}
			if(outPutStream!=null)
			{
				outPutStream.close();
				LOGGER_EVENT.log(Level.INFO,"outPutStream.close() executed...");
			}
			if(hdfsOut!=null)
			{
				hdfsOut.close();
				LOGGER_EVENT.log(Level.INFO,"hdfsOut.close() executed...");
			}
			if(fs!=null)
			{
				fs.close();
				LOGGER_EVENT.log(Level.INFO,"fs.close() executed...");
			}
			if(conn!=null)
			{
				conn.close();
				LOGGER_EVENT.log(Level.INFO,"conn.close() executed...");
			}
			if(conf!=null)
			{
				conf.clear();
				LOGGER_EVENT.log(Level.INFO,"conf.close() executed...");
			}
			if(fh_pvlu !=null)
			{
				LOGGER_EVENT.log(Level.INFO," Executing fh_pvlu.flush() & fh_pvlu.close()...");
				fh_pvlu.flush();
				fh_pvlu.close();
			}

		}catch(Exception e1) {
			e1.printStackTrace();
			throw new Exception("SEVERE:Exception in releaseResources()...."+e1.getMessage());
		}
	}



	public static void main(String args[]){
		try {
			if(args.length !=3) {
				throw new IOException("SEVERE:PVLU JAVA process Requires[filePath, newUPD, V_READ_DATE], number of passed args not matching with expected..."); 
			}else {



				filePath = new Path(args[0]);
				jdCSVFilePath = new Path(args[0] + "_JOBDETAILS");
				new_UPD=args[1];
				V_READ_DATE=args[2]+" +0000";
				//Initialize the LogFile for PVLU,PMLU,MASTER,connection
				getResources(args[2]);
				printInitialValues();
				LOGGER_EVENT.log(Level.INFO,"Log file initiated for the day:- "+new_UPD);
				LOGGER_EVENT.log(Level.INFO,"Input file path: "+filePath);
				LOGGER_EVENT.log(Level.INFO,"Input Error CSV file path: "+jdCSVFilePath);
				LOGGER_EVENT.log(Level.INFO,"Input new upd: "+args[1]);
				LOGGER_EVENT.log(Level.INFO,"Input new v_read_date: "+V_READ_DATE);
				//PVLU.wf_id=""
				//ExecutorService pool;
				//conn = ConnectionFactory.createConnection(conf, pool);//createConnection(configuration); 
				//Call applyUpdates to process all blms exchange events.
				applyUpdates();
				//Release Resources held by this Process.
				LOGGER_EVENT.log(Level.INFO,"Job finished..Calling releaseResources() as normal call..");
				releaseResources();
				System.out.println("END OF JOB...........");
				System.exit(0);
			}
		}catch(Exception e1){		//releaseResources();
		e1.printStackTrace();
		System.out.println("SEVERE:Unhandled Exception:"+e1.getMessage());
		System.exit(1);

		}
	}
}




