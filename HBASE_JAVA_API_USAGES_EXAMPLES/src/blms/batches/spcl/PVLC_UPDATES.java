/*
 * Description		: Java Program to handle BLMS PVLC event.
 * Command to Run	: java -cp `hadoop classpath`:`hbase classpath` blms/batches/spcl/PVLC_UPDATES '<PVLC_JAVA_INPUT_PATH>' '<UPDATE_DATE>'
 * Author			: Yogesh Shinde
 */

/**
 * Provides the classes to handle BLMS special events
 * @since 1.0
 */
package blms.batches.spcl;

import java.io.BufferedReader;
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
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.security.UserGroupInformation;

/**
 * <h1>BLMS Special Event: PVLC</h1>
 * The PVLC_UPDATES program process the PVLC records
 * @author  Yogesh Shinde
 * @version 1.0
 * @since   2018-03-28
 */
public class PVLC_UPDATES {
	/**
	 * Holds the configuration values
	 */
	private static Properties configProp = new Properties();
	/**
	 * Holds the PACK_INFO table row
	 */
	static Map<byte[], byte[]> PACK_INFO_TBL = new HashMap<byte[], byte[]>();
	/**
	 * Holds the PROBE table row
	 */
	static Map<byte[], byte[]> BLMS_PROBE_TBL = new HashMap<byte[], byte[]>();
	/**
	 * Holds the VIN_IDX table row
	 */
	static Map<byte[], byte[]> VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	/**
	 * Holds the PACK_MAP table row
	 */
	static Map<byte[], byte[]> PACK_MAP_TBL = new HashMap<byte[], byte[]>();
	/**
	 * Holds the JOB_DETAILS table records (success status and error cases)
	 */
	static Map<Integer, String> JOB_DETAILS = new HashMap<Integer, String>();
	/**
	 * Counter to maintain the current line processing
	 */
	static int line_cnt = 0;
	/**
	 * Holds the UPDATE_DATE value from argument list
	 */
	private static String new_UPD;
	/**
	 * Holds the input path for PVLC from argument list
	 */
	private static Path filePath;
	/**
	 * Holds the path for JOB_DETAILS CSV file
	 */
	private static Path jdCSVFilePath;
	/**
	 * LogFile Handler
	 */
	private static FileHandler fh_pvlc;
	/**
	 * Hadoop File System to access HDFS files
	 */
	private static FileSystem fs;
	/**
	 * To read the HDFS file.
	 */
	private static BufferedReader br;
	/**
	 * To write into HDFS file
	 */
	private static FSDataOutputStream outPutStream;
	/**
	 * Holds the PROBE key
	 */
	private static String ProbeKey;
	/**
	 * Holds the new PROBE_SEARCH1 key
	 */
	private static String newSearch1Key;
	/**
	 * Holds the old PROBE_SEARCH1 key
	 */
	private static String oldSearch1Key;
	/**
	 * Holds the new PROBE_SEARCH2 key
	 */
	private static String newSearch2Key;
	/**
	 * Holds the old PROBE_SEARCH2 key
	 */
	private static String oldSearch2Key;
	/**
	 * Holds the new PROBE_SEARCH3 key
	 */
	private static String newSearch3Key;
	/**
	 * Holds the old PROBE_SEARCH3 key
	 */
	private static String oldSearch3Key;
	/**
	 * Holds the constant PVLC EVENT_ID
	 */
	private static String pvlc_event_id;
	/**
	 * Holds the PK_INFO HBase table column family
	 */
	private static byte[] cf_pk;
	/**
	 * Holds the IDX HBase tables columns family
	 */
	private static byte[] cf_idx;
	/**
	 * Holds the PROBE HBase table common family
	 */
	private static byte[] cf_cmn;
	/**
	 * Holds the PK_INFO required HBase Qualifiers
	 */
	private static byte[] pkinfo_blms_id, pkinfo_vin, pkinfo_upd, pkinfo_orignalKey, pkinfo_alliancePk, pkinfo_pvdate, pkinfo_nissanPack, pkinfo_PackDateKey, 
	pkinfo_Timestamp, pkinfo_byte_buyer_code;
	/**
	 * Holds the PACK_MAP required HBase Qualifiers
	 */
	private static byte[] pkmap_old_np, pkmap_old_np_idate, pkmap_new_np;
	/**
	 * Holds the HBase NameSpace
	 */
	private static String hbaseNameSpace;
	/**
	 * Holds the PROBE and its INDEX table names
	 */
	private static String blms_probe, blms_probe_search1, blms_probe_search2, blms_probe_search3;
	/**
	 * Holds the PACK_INFO and its INDEX table names
	 */
	private static String blms_pack_info, blms_pack_info_pack_map, blms_pack_info_upd_idx, blms_pack_info_vin_idx;
	/**
	 * Holds the HDFS configuration
	 */
	private static String defaultFS, hadoopSecurityAuthentication;
	/**
	 * Holds the HBase configuration
	 */
	private static String hbaseZookeeperPort, hbaseZookeeperQuorum1, hbaseZnodeParent, hbaseMasterPort, hbaseMaster, hbaseRpcTimeout, hbaseClientScannerTimeout, 
	hbaseCellsScannedPerHeartBeatCheck, hbaseClientOperationTimeout;

	private static String V_READ_DATE, PkInfo_VAL_s, PkInfo_VAL_t, PkInfo_VAL_v, PkInfo_VAL_w, PkInfo_VAL_x, PkInfo_VAL_h, PkInfo_VAL_i, PkInfo_VAL_j;
	private  static byte[] PkInfo_FLD_q, PkInfo_FLD_r, PkInfo_FLD_s, PkInfo_FLD_t, PkInfo_FLD_u, PkInfo_FLD_v, PkInfo_FLD_w, PkInfo_FLD_x, PkInfo_FLD_h, PkInfo_FLD_i, PkInfo_FLD_j;

	/**
	 * Holds the PVLC Log file name
	 */
	private static String PVLC_UPDATES_LOG;
	/**
	 * Logger for PVLC processing
	 */
	private static Logger LOGGER_PVLC;
	/**
	 * Holds the HBase connection
	 */
	static Connection conn = null;
	private static final String PropertyFileName = "/home/asp20571bdp/DEV/BDP_BATCHES/COMMON_UTILS/PROPERTIES/conf_blmb0102.properties";

	/**
	 * Initializes all the configuration variables from the property file
	 */
	static {
		//InputStream in = PVLC_UPDATES.class.getClassLoader().getResourceAsStream("blms/batches/spcl/conf_blmb0102.properties");
		try {
			//InputStream in = new FileInputStream("../../../COMMON_UTILS/PROPERTIES/conf_blmb0102.properties");
			InputStream in = new FileInputStream(PropertyFileName);
			configProp.load(in);

			defaultFS = configProp.getProperty("defaultFS");
			hadoopSecurityAuthentication = configProp.getProperty("hadoopSecurityAuthentication");

			hbaseZookeeperPort = configProp.getProperty("hbaseZookeeperPort");
			hbaseZookeeperQuorum1 = configProp.getProperty("hbaseZookeeperQuorum1");
			hbaseZnodeParent = configProp.getProperty("hbaseZnodeParent");
			hbaseMasterPort = configProp.getProperty("hbaseMasterPort");
			hbaseMaster = configProp.getProperty("hbaseMaster");
			hbaseRpcTimeout = configProp.getProperty("hbaseRpcTimeout");
			hbaseClientScannerTimeout = configProp.getProperty("hbaseClientScannerTimeout");
			hbaseCellsScannedPerHeartBeatCheck = configProp.getProperty("hbaseCellsScannedPerHeartBeatCheck");
			hbaseClientOperationTimeout = configProp.getProperty("hbaseClientOperationTimeout");

			cf_pk = Bytes.toBytes(configProp.getProperty("cf_pk"));
			cf_idx = Bytes.toBytes(configProp.getProperty("cf_idx"));
			cf_cmn = Bytes.toBytes(configProp.getProperty("cf_cmn"));

			pkinfo_blms_id = Bytes.toBytes(configProp.getProperty("pkinfo_blms_id"));
			pkinfo_vin = Bytes.toBytes(configProp.getProperty("pkinfo_vin"));
			pkinfo_upd = Bytes.toBytes(configProp.getProperty("pkinfo_upd"));
			pkinfo_orignalKey = Bytes.toBytes(configProp.getProperty("pkinfo_orignalKey"));
			pkinfo_alliancePk = Bytes.toBytes(configProp.getProperty("pkinfo_alliancePk"));
			pkinfo_pvdate = Bytes.toBytes(configProp.getProperty("pkinfo_pvdate"));
			pkinfo_nissanPack = Bytes.toBytes(configProp.getProperty("pkinfo_nissanPack"));			
			pkinfo_PackDateKey = Bytes.toBytes(configProp.getProperty("pkinfo_PackDateKey"));
			pkinfo_Timestamp = Bytes.toBytes(configProp.getProperty("pkinfo_Timestamp"));
			pkinfo_byte_buyer_code = Bytes.toBytes(configProp.getProperty("pkinfo_byte_buyer_code"));

			pkmap_old_np = Bytes.toBytes(configProp.getProperty("pkmap_old_np"));
			pkmap_old_np_idate = Bytes.toBytes(configProp.getProperty("pkmap_old_np_idate"));
			pkmap_new_np = Bytes.toBytes(configProp.getProperty("pkmap_new_np"));

			hbaseNameSpace = configProp.getProperty("hbaseNameSpace");
			blms_probe = configProp.getProperty("blms_probe");
			blms_probe_search1 = configProp.getProperty("blms_probe_search1");
			blms_probe_search2 = configProp.getProperty("blms_probe_search2");
			blms_probe_search3 = configProp.getProperty("blms_probe_search3");
			blms_pack_info = configProp.getProperty("blms_pack_info");
			blms_pack_info_pack_map = configProp.getProperty("blms_pack_info_pack_map");
			blms_pack_info_upd_idx = configProp.getProperty("blms_pack_info_upd_idx");
			blms_pack_info_vin_idx = configProp.getProperty("blms_pack_info_vin_idx");

			pvlc_event_id = configProp.getProperty("pvlc_event_id");
			PVLC_UPDATES_LOG = configProp.getProperty("PVLC_UPDATES_LOG");

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

			//LOGGER_PVLC = Logger.getLogger(configProp.getProperty("LOGGER_PVLC"));
			LOGGER_PVLC = Logger.getLogger(PVLC_UPDATES.class.getName());
			in.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("SEVERE:UnHandled Exception in static intilization block..."+e.getMessage());
			System.exit(1);
		}
	}


	private static void printInitialValues() {

		LOGGER_PVLC.log(Level.INFO,"PropertyFileName: "+PropertyFileName);
		LOGGER_PVLC.log(Level.INFO,"defaultFS: "+defaultFS);
		LOGGER_PVLC.log(Level.INFO,"hbaseZookeeperPort: "+hbaseZookeeperPort);
		LOGGER_PVLC.log(Level.INFO,"hbaseZookeeperQuorum1: "+hbaseZookeeperQuorum1);
		LOGGER_PVLC.log(Level.INFO,"hbaseZnodeParent: "+hbaseZnodeParent);
		LOGGER_PVLC.log(Level.INFO,"hbaseMasterPort: "+hbaseMasterPort);
		LOGGER_PVLC.log(Level.INFO,"hbaseMaster: "+hbaseMaster);
		LOGGER_PVLC.log(Level.INFO,"hbaseRpcTimeout: "+hbaseRpcTimeout);
		LOGGER_PVLC.log(Level.INFO,"hbaseClientScannerTimeout: "+hbaseClientScannerTimeout);
		LOGGER_PVLC.log(Level.INFO,"hbaseCellsScannedPerHeartBeatCheck: "+hbaseCellsScannedPerHeartBeatCheck);
		LOGGER_PVLC.log(Level.INFO,"hbaseClientOperationTimeout: "+hbaseClientOperationTimeout);
		LOGGER_PVLC.log(Level.INFO,"hadoopSecurityAuthentication: "+hadoopSecurityAuthentication);
		LOGGER_PVLC.log(Level.INFO,"PVLC_UPDATES_LOG: "+PVLC_UPDATES_LOG);
		LOGGER_PVLC.log(Level.INFO,"pvlc_event_id: "+pvlc_event_id);
		LOGGER_PVLC.log(Level.INFO,"cf_pk: "+Bytes.toString(cf_pk));
		LOGGER_PVLC.log(Level.INFO,"cf_idx: "+Bytes.toString(cf_idx));
		LOGGER_PVLC.log(Level.INFO,"cf_cmn: "+Bytes.toString(cf_cmn));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_blms_id: "+Bytes.toString(pkinfo_blms_id));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_vin: "+Bytes.toString(pkinfo_vin));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_upd: "+Bytes.toString(pkinfo_upd));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_orignalKey: "+Bytes.toString(pkinfo_orignalKey));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_alliancePk: "+Bytes.toString(pkinfo_alliancePk));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_pvdate: "+Bytes.toString(pkinfo_pvdate));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_nissanPack: "+Bytes.toString(pkinfo_nissanPack));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_PackDateKey: "+Bytes.toString(pkinfo_PackDateKey));
		LOGGER_PVLC.log(Level.INFO,"pkinfo_Timestamp: "+Bytes.toString(pkinfo_Timestamp));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_q: "+Bytes.toString(PkInfo_FLD_q));
		LOGGER_PVLC.log(Level.INFO,"hbaseNameSpace: "+hbaseNameSpace);
		LOGGER_PVLC.log(Level.INFO,"blms_probe: "+blms_probe);
		LOGGER_PVLC.log(Level.INFO,"blms_probe_search1: "+blms_probe_search1);
		LOGGER_PVLC.log(Level.INFO,"blms_probe_search2: "+blms_probe_search2);
		LOGGER_PVLC.log(Level.INFO,"blms_probe_search3: "+blms_probe_search3);
		LOGGER_PVLC.log(Level.INFO,"blms_pack_info: "+blms_pack_info);
		LOGGER_PVLC.log(Level.INFO,"blms_pack_info_upd_idx: "+blms_pack_info_upd_idx);
		LOGGER_PVLC.log(Level.INFO,"blms_pack_info_vin_idx: "+blms_pack_info_vin_idx);

		LOGGER_PVLC.log(Level.INFO,"pkinfo_byte_buyer_code: "+Bytes.toString(pkinfo_byte_buyer_code));
		LOGGER_PVLC.log(Level.INFO,"pkmap_old_np: "+Bytes.toString(pkmap_old_np));
		LOGGER_PVLC.log(Level.INFO,"pkmap_old_np_idate: "+Bytes.toString(pkmap_old_np_idate));
		LOGGER_PVLC.log(Level.INFO,"pkmap_new_np: "+Bytes.toString(pkmap_new_np));
		LOGGER_PVLC.log(Level.INFO,"blms_pack_info_vin_idx: "+blms_pack_info_vin_idx);


		LOGGER_PVLC.log(Level.INFO,"AMO COLUMN LIST FROM PROPERTY FILE(Displaying..) ");
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_s: "+PkInfo_VAL_s);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_t: "+PkInfo_VAL_t);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_v: "+PkInfo_VAL_v);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_w: "+PkInfo_VAL_w);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_x: "+PkInfo_VAL_x);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_h: "+PkInfo_VAL_h);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_i: "+PkInfo_VAL_i);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_VAL_j: "+PkInfo_VAL_j);
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_q: "+Bytes.toString(PkInfo_FLD_q));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_r: "+Bytes.toString(PkInfo_FLD_r));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_s: "+Bytes.toString(PkInfo_FLD_s));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_t: "+Bytes.toString(PkInfo_FLD_t));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_u: "+Bytes.toString(PkInfo_FLD_u));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_v: "+Bytes.toString(PkInfo_FLD_v));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_w: "+Bytes.toString(PkInfo_FLD_w));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_x: "+Bytes.toString(PkInfo_FLD_x));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_h: "+Bytes.toString(PkInfo_FLD_h));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_i: "+Bytes.toString(PkInfo_FLD_i));
		LOGGER_PVLC.log(Level.INFO,"PkInfo_FLD_j: "+Bytes.toString(PkInfo_FLD_j));
	}


	/**
	 * To put the row in HBase table
	 * @param PutIntoTable - Table name to insert the row.
	 * @param PutRowList - Row to insert
	 * @throws Exception 
	 */
	public static void putInTable(String PutIntoTable, Put PutRowList) throws Exception {
		Table blms_tbl_outConn = null;
		try {
			TableName blms_tbl_out = TableName.valueOf(hbaseNameSpace + ":" + PutIntoTable);
			blms_tbl_outConn =  conn.getTable(blms_tbl_out);
			blms_tbl_outConn.put(PutRowList);
		} catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_PVLC.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_PVLC.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				LOGGER_PVLC.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_PVLC.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
			} 
			LOGGER_PVLC.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_PVLC.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 

			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_PVLC.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}
	}//end putTable()
	/**
	 * To put the list of rows in HBase table (batch load)
	 * @param PutIntoTable - Table name to insert rows.
	 * @param PutRowList - List of rows to insert
	 * @throws Exception 
	 */
	public static void putInTable(String PutIntoTable, List<Put> PutRowList) throws Exception {
		Table blms_tbl_outConn = null;
		try {
			TableName blms_tbl_out = TableName.valueOf(hbaseNameSpace+":" + PutIntoTable);
			blms_tbl_outConn =  conn.getTable(blms_tbl_out);
			blms_tbl_outConn.put(PutRowList);
		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_PVLC.log(Level.SEVERE,"Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_PVLC.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				//System.out.println("Cause[" + n + "]: " + e.getCause(n));
				LOGGER_PVLC.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				//System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_PVLC.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
				//System.out.println("Row[" + n + "]: " + e.getRow(n)); // ErrorPut Gain access to the failed operation.
			} 
			LOGGER_PVLC.log(Level.SEVERE, "Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_PVLC.log(Level.SEVERE, "Description: " + e.getExhaustiveDescription()); 

			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_PVLC.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable()...."+e.getMessage());
		}
	}//end putTable()
	/**
	 * To delete list of rows from HBase table
	 * @param DeleteFromTable - Name of table
	 * @param DeleteRowList - List of rows to delete
	 * @throws Exception 
	 */
	public static void deleteFromTable(String DeleteFromTable, List<Delete> DeleteRowList) throws Exception {
		Table blms_tbl_outConn = null;
		try {
			TableName blms_tbl_out = TableName.valueOf(hbaseNameSpace + ":" + DeleteFromTable);
			blms_tbl_outConn =  conn.getTable(blms_tbl_out);
			blms_tbl_outConn.delete(DeleteRowList);
		}catch (RetriesExhaustedWithDetailsException e) { 
			int numErrors = e.getNumExceptions(); // Error Handle failed operations. 
			LOGGER_PVLC.log(Level.SEVERE, "Number of exceptions: " + numErrors); 
			for (int n = 0; n < numErrors; n++) { 
				LOGGER_PVLC.log(Level.SEVERE,"Cause[" + n + "]: " + e.getCause(n));
				LOGGER_PVLC.log(Level.SEVERE,"Hostname[" + n + "]: " + e.getHostnamePort(n));
				LOGGER_PVLC.log(Level.SEVERE,"Row[" + n + "]: " + e.getRow(n));
			} 
			LOGGER_PVLC.log(Level.SEVERE,"Cluster issues: " + e.mayHaveClusterIssues()); 
			LOGGER_PVLC.log(Level.SEVERE,"Description: " + e.getExhaustiveDescription()); 

			throw new Exception("SEVERE:Exception in deleteFromTable()...."+e.getMessage());
		}catch(Exception e){
			LOGGER_PVLC.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in deleteFromTable()...."+e.getMessage());
		}
	}//end deleteFromTable
	/**
	 * Entry point of PVLC input file processing 
	 * @param filePath - PVLC input path
	 * @param jdCSVFilePath - CSV file path to log success and error records
	 * @throws Exception 
	 */
	public static void applyUpdates() throws Exception {
		try {
			Configuration conf = getHDFSConf();
			fs = FileSystem.get(conf);
			LOGGER_PVLC.log(Level.INFO, "HDFS file path for PVLC input:-" + filePath); 
			outPutStream = fs.create(jdCSVFilePath, true);
			if(!fs.exists(filePath)) {
				LOGGER_PVLC.log(Level.SEVERE,"INVALID ARGUMENTS: Path doest not exist..");
				throw new Exception("SEVERE:INVALID ARGUMENTS: Path doest not exist..");
				//LOGGER_EVENT.log(Level.INFO,"INVALID ARGUMENTS: Path doest not exist..");
				//releaseResources();
				//System.exit(1);
			}
			FileStatus[] status = fs.listStatus(filePath);
			String line = null;
			for (FileStatus fileStatus : status) {
				br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
				while ((line = br.readLine()) != null) {
					line_cnt++;
					LOGGER_PVLC.log(Level.INFO, "input lineCnt taken for processing: " + line_cnt);
					String token[] =  line.split("\t");
					LOGGER_PVLC.log(Level.INFO,"token lenght:-" + token.length);
					if(token != null) {
						/*
						 * PVLC_INPUT_EVENT_ID, PVLC_INPUT_Battery_Pack_ID, PVLC_INPUT_Battery_VIN_Exchange_Date_utc,
						 * PVLC_INPUT_VIN, PVLC_INPUT_BUYER_CODE, PVLC_INPUT_Battery_VIN_Exchange_Date_ANYTZ,
						 * read_date, update_date, ref_file_name, src_file_path, PVLC_INPUT_FILE_NAME, src_processed_time, wf_id
						 */

						LOGGER_PVLC.log(Level.INFO, "Input Event Record for Processing: " + token[0] + "\t" + token[1] + "\t" + token[2] + "\t" + token[3]+ "\t" + token[4]);
						if(token[0].equals(pvlc_event_id)) {
							LOGGER_PVLC.log(Level.INFO, "PVLC Entry For Pack: " + token[1]);
							LOGGER_PVLC.log(Level.INFO, "Calling pvlcApply()");
								pvlcApply(token);
								LOGGER_PVLC.log(Level.INFO, "pvlcApply() Process: Done for Event Line number: " + line_cnt);
						} else {
							LOGGER_PVLC.log(Level.INFO, "Wrong Entry Record for PVLC: " + token[1]);
						}
					}
					ProcessJDRecords(line, outPutStream);
				}
			}
			//	LOGGER_PVLC.log(Level.INFO, "JobDetail Log successfully created on HDFS: " + jdCSVFilePath);
		}catch(Exception e) {	//LOGGER_EVENT.log(Level.SEVERE,"Error In getHDFSConf(), failed to read the hdfs filesystem using provided conf..");
			LOGGER_PVLC.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in applyUpdates()....."+e.getMessage());
		}
		LOGGER_PVLC.log(Level.INFO,"JobDetail Log successfully created on HDFS : "+jdCSVFilePath);
	}
	/**
	 * To get the latest record from the HBase table
	 * @param tableName - Name of the table to fetch he record
	 * @param prefixFilterValue - Prefix value filter
	 * @return the result object of a latest record
	 * @throws IOException if any issue with HBase table connection
	 */
	/* Old Function which was using the hbase feature : reverse=true to get the latest record for each RowPrefix.
	public static Result getLatestRecord(String tableName, String prefixFilterValue) throws IOException {
		LOGGER_PVLC.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
		TableName blms_tbl = TableName.valueOf(hbaseNameSpace + ":" + tableName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		Scan scan = new Scan(); 
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		allFilters.addFilter(new PrefixFilter(Bytes.toBytes(prefixFilterValue)));
		scan.setFilter(allFilters);
		scan.setReversed(true); //Read the latest available key and value
		scan.setMaxResultSize(1);
		ResultScanner scanner = blms_tblConn.getScanner(scan);
		Result result = scanner.next();
		scanner.close(); //Scanner closed
		blms_tblConn.close();
		if(result != null)
			LOGGER_PVLC.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(result.getRow()));
		return result;
	}*/

	public static Result getLatestRecord(String tableName, String prefixFilterValue) throws IOException {

		//List keys = new ArrayList<Result>();
		Result LastRow=null ;

		LOGGER_PVLC.log(Level.INFO, "Fetching Latest Record for Table: " + tableName);
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
			LOGGER_PVLC.log(Level.INFO, "Fetched Latest Record for Table: " + tableName + ", Record: " + Bytes.toString(LastRow.getRow()));
		else
			LOGGER_PVLC.log(Level.INFO, "Record not Found....");

		return LastRow;
	}


	/**
	 * Removes the pack of VIN
	 * @param vin - VIN to remove the pack
	 * @throws Exception 
	 */
	private static void removeVINFromOldPack(String vin) throws Exception {

		Result vin_idx_result = getLatestRecord(blms_pack_info_vin_idx, vin);

		if(vin_idx_result != null) {

			LOGGER_PVLC.log(Level.INFO, "Found existing entry for VIN: " + vin);
			LOGGER_PVLC.log(Level.INFO, "Pack change for VIN: " + vin);
			VIN_IDX_TBL = vin_idx_result.getFamilyMap(cf_idx);
			String nissan_pack = Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack));			

			if(nissan_pack != null && !nissan_pack.isEmpty()) {

				LOGGER_PVLC.log(Level.INFO, "Removing Pack from PACK_MAP for VIN: " + vin);
				Result pack_map_result = getLatestRecord(blms_pack_info_pack_map, nissan_pack);

				if(pack_map_result!=null) {

					PACK_MAP_TBL = pack_map_result.getFamilyMap(cf_idx);
					LOGGER_PVLC.log(Level.INFO, "Preparing new record for packMap..");
					PACK_MAP_TBL.put(pkinfo_vin, Bytes.toBytes(""));
					PACK_MAP_TBL.put(pkinfo_pvdate, Bytes.toBytes(""));
					PACK_MAP_TBL.put(pkinfo_byte_buyer_code, Bytes.toBytes("")); //re-setting buyer code to null
					PACK_MAP_TBL.put(pkinfo_upd, Bytes.toBytes(new_UPD));
					byte [] newPackMapKey = Bytes.toBytes(nissan_pack.concat(new_UPD));
					Put NewPackMap_kv = new Put(newPackMapKey);

					for (Entry<byte[], byte[]> entry : PACK_MAP_TBL.entrySet()) {
						byte[] key = entry.getKey();
						byte[] value = entry.getValue();
						NewPackMap_kv.addColumn(cf_idx, key, value);
					}

					LOGGER_PVLC.log(Level.INFO,"blms_pack_info_pack_map tbl Insert Started for the event Line Number: " + line_cnt);
					putInTable(blms_pack_info_pack_map, NewPackMap_kv);
					LOGGER_PVLC.log(Level.INFO, "blms_pack_info_pack_map tbl Insert Finished for the event Line Number: " + line_cnt);
					LOGGER_PVLC.log(Level.INFO, "End of new record for packMap insert:" + Bytes.toString(newPackMapKey));
				}
			}

			String alliance_pack = Bytes.toString(VIN_IDX_TBL.get(pkinfo_alliancePk));

			if(alliance_pack != null && !alliance_pack.isEmpty()) {
				LOGGER_PVLC.log(Level.INFO, "Removing Pack from PACK_INFO for VIN: " + vin);
				//1. Update the PACK_INFO table

				Result pack_info_result = getLatestRecord(blms_pack_info, alliance_pack);
				PACK_INFO_TBL = pack_info_result.getFamilyMap(cf_pk);				
				LOGGER_PVLC.log(Level.INFO, "Preparing new record for packInfo..");
				PACK_INFO_TBL.put(pkinfo_vin, Bytes.toBytes(""));  //Removing VIN from the pack
				PACK_INFO_TBL.put(pkinfo_pvdate, Bytes.toBytes("")); //Removing Battery setting Date from the Pack
				PACK_INFO_TBL.put(pkinfo_byte_buyer_code, Bytes.toBytes("")); //Removing Battery setting Date from the Pack
				PACK_INFO_TBL.put(pkinfo_upd, Bytes.toBytes(new_UPD));   // new UPD date for record
				byte [] newPackInfoKey = Bytes.toBytes(alliance_pack.concat(new_UPD));
				Put NewPackInfo_kv = new Put(newPackInfoKey);
				for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
					byte[] key = entry.getKey();
					byte[] value = entry.getValue();
					NewPackInfo_kv.addColumn(cf_pk, key, value);
				}
				LOGGER_PVLC.log(Level.INFO,"blms_pack_info tbl Insert Started for the event Line Number: "+line_cnt);
				putInTable(blms_pack_info, NewPackInfo_kv);
				LOGGER_PVLC.log(Level.INFO, "blms_pack_info tbl Insert Finished for the event Line Number: " + line_cnt);
				LOGGER_PVLC.log(Level.INFO, "End of new record for packInfo insert:" + Bytes.toString(newPackInfoKey));
				//2. Update the UPD_IDX table
				LOGGER_PVLC.log(Level.INFO,"Prepare new record for:upd_idx_tbl: " + alliance_pack);
				byte [] newUpdKey = Bytes.toBytes(new_UPD.concat(alliance_pack));
				Put NewUpdIdx_kv = new Put(newUpdKey);  //token[1] <- AP
				NewUpdIdx_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));
				NewUpdIdx_kv.addColumn(cf_idx, pkinfo_alliancePk, PACK_INFO_TBL.get(pkinfo_orignalKey));
				LOGGER_PVLC.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Started for the event Line Number: " + line_cnt);
				putInTable(blms_pack_info_upd_idx, NewUpdIdx_kv);
				LOGGER_PVLC.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Finished for the event Line Number: " + line_cnt);
				LOGGER_PVLC.log(Level.INFO,"End UPD_idx Updated for New record insert: " + Bytes.toString(newUpdKey));
			}
		} else {
			LOGGER_PVLC.log(Level.INFO, "New Entry for VIN: " + vin);
		}
	}
	/**
	 * Actual PVLC event processing (includes all cases)
	 * @param token - PVLC input record
	 * @param blms_pack_info - Name of the BLMS PACK_INFO table
	 * @param blms_pack_info_pack_map - Name of the BLMS PACK_MAP table
	 * @param blms_pack_info_vin_idx - Name of the BLMS VIN_IDX table
	 * @throws Exception 
	 */
	public static void pvlcApply(String[] token) throws Exception  {

		boolean retrans_flag =false;  //flag used to identidy the retansmitted pvlc , used to distiguish between : packChnage and Retransmission with some chnaged values.

		LOGGER_PVLC.log(Level.INFO, "Inside Of pvlcApply() for Pack: " + token[1]);
		//record to get existing mapping for pack change check : last PVLC check for VIN--> nissanpack	
		Result vin_idx_result = getLatestRecord(blms_pack_info_vin_idx, token[3]);
		if (vin_idx_result != null) {
			VIN_IDX_TBL = vin_idx_result.getFamilyMap(cf_idx);
		} 
		//record to get existing mapping for pack change check : last PVLC check for  nissanpack --> vin
		Result pack_map_result = getLatestRecord(blms_pack_info_pack_map, token[1]);  
		if (pack_map_result != null) { //last PVLC or NPC already arrived.
			LOGGER_PVLC.log(Level.INFO, "Pack found in PACK_MAP(last PVLC or NPC already arrived): " + token[1]);
			PACK_MAP_TBL = pack_map_result.getFamilyMap(cf_idx);
			LOGGER_PVLC.log(Level.INFO, "PACK_MAP Entry (existing key): " + Bytes.toString(pack_map_result.getRow()));

			String alliance_pack = Bytes.toString(PACK_MAP_TBL.get(pkinfo_alliancePk));
			String packmap_tbl_overseasBuyerCode = Bytes.toString(PACK_MAP_TBL.get(pkinfo_byte_buyer_code)); //set in static
			String packmap_tbl_BatterySettingDate = Bytes.toString(PACK_MAP_TBL.get(pkinfo_pvdate)); //set in static
			String packmap_tbl_vin = Bytes.toString(PACK_MAP_TBL.get(pkinfo_vin));

			//token[2] --> BatterySettingDate
			//token[4] --> Arrived overseasBuyerCode
			//token[3] --> VIN
			/*			
			LOGGER_PVLC.log(Level.INFO, "#####################");
			if((packmap_tbl_vin !=null && !packmap_tbl_vin.isEmpty()) && packmap_tbl_vin.equals(token[3]))		
				LOGGER_PVLC.log(Level.INFO, "token3: " + token[3]);
			if(packmap_tbl_overseasBuyerCode == null || packmap_tbl_overseasBuyerCode.isEmpty() || packmap_tbl_overseasBuyerCode.equals(token[4]))
				LOGGER_PVLC.log(Level.INFO, "token4: " + token[4]);
			if(packmap_tbl_BatterySettingDate == token[2])
				LOGGER_PVLC.log(Level.INFO, "token2: " + token[2]);
			 */
			LOGGER_PVLC.log(Level.INFO, "#####################");
			LOGGER_PVLC.log(Level.INFO, "All Comapred Token values, TOKEN3:" + token[3]+" tbl:packmap_tbl_vin:"+packmap_tbl_vin);
			LOGGER_PVLC.log(Level.INFO, "All Comapred Token values, TOKEN4:" + token[4]+" tbl:packmap_tbl_overseasBuyerCode:"+packmap_tbl_overseasBuyerCode);
			LOGGER_PVLC.log(Level.INFO, "All Comapred Token values, TOKEN2:" + token[2]+" tbl:packmap_tbl_BatterySettingDate:"+packmap_tbl_BatterySettingDate);
			LOGGER_PVLC.log(Level.INFO, "#####################");

			if(((packmap_tbl_vin !=null && !packmap_tbl_vin.isEmpty()) && packmap_tbl_vin.equals(token[3]) ) &&
					((packmap_tbl_BatterySettingDate !=null && !packmap_tbl_BatterySettingDate.isEmpty()) && packmap_tbl_BatterySettingDate.equals(token[2]) ) && 
					(((packmap_tbl_overseasBuyerCode !=null && !packmap_tbl_overseasBuyerCode.isEmpty()) && packmap_tbl_overseasBuyerCode.equals(token[4])) || 
							((packmap_tbl_overseasBuyerCode ==null || packmap_tbl_overseasBuyerCode.isEmpty()) && (token[4] == null || token[4].isEmpty())))) {

				//if(packmap_tbl_vin == token[3] && packmap_tbl_overseasBuyerCode ==token[4] && packmap_tbl_BatterySettingDate == token[2]) {

				LOGGER_PVLC.log(Level.INFO, "Arrived VIN: " + token[3] + " might be same with existing value in PACK_MAP_TBL (last PVLC record) : "+packmap_tbl_vin);
				LOGGER_PVLC.log(Level.INFO, "Arrived OverSeasBuyerCode: " + token[4] + " might be  same with existing value in PACK_MAP_TBL : "+packmap_tbl_overseasBuyerCode);
				LOGGER_PVLC.log(Level.INFO, "Arrived BatterySettingDate: " + token[2] + " might be  same with existing value in PACK_MAP_TBL : "+packmap_tbl_BatterySettingDate);

				LOGGER_PVLC.log(Level.INFO, "ERROR CASE: PVLC Re-Transmitted with same Record..");
				JOB_DETAILS.put(line_cnt, "ERROR CASE: PVLC Re-Transmitted with same Record..");

			}else if((packmap_tbl_vin !=null && !packmap_tbl_vin.isEmpty()) && !packmap_tbl_vin.equals(token[3])) { //condition: Error case: different VIN, same pack arrived.

				LOGGER_PVLC.log(Level.INFO, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN: " + packmap_tbl_vin);
				JOB_DETAILS.put(line_cnt, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN: " + packmap_tbl_vin);

			}else if(alliance_pack != null && !alliance_pack.isEmpty()) {//Confirm NPC arrived by checking AP in PackMap to start the Backfilling of tbls.
				/*Either :1. PVLC new event. <--else of this block
								   2. PVLC Retransmitted with different buyer code & Same VIN. OR 
		 				   		   3. PVLC Retransmitted with different PVdate & Same VIN.
				 */
				LOGGER_PVLC.log(Level.INFO, "AlliancePack available in PACK_MAP(NPC already arrived): " + alliance_pack);
				Result pack_info_result = getLatestRecord(blms_pack_info, alliance_pack);

				if(pack_info_result != null) {

					LOGGER_PVLC.log(Level.INFO, "Found Pack entry in PACK_INFO: " + alliance_pack);
					PACK_INFO_TBL = pack_info_result.getFamilyMap(cf_pk);
					String pkinfoVin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
					String pkinfoNP = Bytes.toString(PACK_INFO_TBL.get(pkinfo_nissanPack));
					if(pkinfoVin == null || pkinfoVin.isEmpty() || pkinfoVin.equals(packmap_tbl_vin)) {

						// Check for VIN in PKINFO : different VIN for same Pack arrived <-- error case.
						LOGGER_PVLC.log(Level.INFO, "PackInfo TBL VIN:-"+ pkinfoVin + "is either empty or It equls to retransmitted VIN value in arrived event: " + token[3]);

						if(pkinfoVin != null && !pkinfoVin.isEmpty() && pkinfoVin.equals(packmap_tbl_vin) && pkinfoNP.equals(token[1]))
						{
							retrans_flag = true;
						}


						if (vin_idx_result != null) {
							LOGGER_PVLC.log(Level.INFO, "PACK_CHANGED for VIN: " + token[3]);
							LOGGER_PVLC.log(Level.INFO, "Removing Old Packs from VIN: " + token[3]);
							removeVINFromOldPack(token[3]);
						}
						LOGGER_PVLC.log(Level.INFO, "Attaching pack: " + token[1] + " to VIN: " + token[3]);
						LOGGER_PVLC.log(Level.INFO, "Preparing new record for packInfo..");
						LOGGER_PVLC.log(Level.INFO, "Read latest PkInfo Record for arrived NissanPack using existing Result: "+ token[1]);
						PACK_INFO_TBL = pack_info_result.getFamilyMap(cf_pk);

						PACK_INFO_TBL.put(pkinfo_vin, Bytes.toBytes(token[3]));  //setting arrived VIN
						PACK_INFO_TBL.put(pkinfo_pvdate, Bytes.toBytes(token[2])); //setting Battery setting Date
						PACK_INFO_TBL.put(pkinfo_byte_buyer_code, Bytes.toBytes(token[4])); //setting buyer code
						PACK_INFO_TBL.put(pkinfo_upd, Bytes.toBytes(new_UPD));   // new UPD date for record

						PACK_INFO_TBL.put(PkInfo_FLD_r, Bytes.toBytes(token[10])); //Set inputEventFilename as pmlu event registering, AMO cols
						PACK_INFO_TBL.put(PkInfo_FLD_q, Bytes.toBytes(V_READ_DATE)); //Set ReadDate for this CSV registration, AMO cols

						byte [] newPackInfoKey = Bytes.toBytes(alliance_pack.concat(new_UPD));
						Put NewPackInfo_kv = new Put(newPackInfoKey);
						for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
							byte[] key = entry.getKey();
							byte[] value = entry.getValue();							
							NewPackInfo_kv.addColumn(cf_pk, key, value);
						}
						putInTable(blms_pack_info, NewPackInfo_kv);
						LOGGER_PVLC.log(Level.INFO, "End of new record for packInfo insert:" + Bytes.toString(newPackInfoKey));
						LOGGER_PVLC.log(Level.INFO, "retrans_flag value:" + retrans_flag);

						LOGGER_PVLC.log(Level.INFO, "calling updatePkInfoIdxTables()....");
						updatePkInfoIdxTables(retrans_flag); //Updating index tables of PACK_INFO, if retrans flag on , dont keep the old pack information while creating new record

						LOGGER_PVLC.log(Level.INFO, "calling updateProbeTables()....");
						updateProbeTables(); //Updating PROBE and its index tables

						JOB_DETAILS.put(line_cnt, "success");
						LOGGER_PVLC.log(Level.INFO, "Set:- JOB_DETAILS:" + String.valueOf(line_cnt) + "=success");

					}// else { //
					//LOGGER_PVLC.log(Level.INFO, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN: " + pkinfoVin);
					//JOB_DETAILS.put(line_cnt, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN: " + pkinfoVin);
					//}
				} else {
					LOGGER_PVLC.log(Level.SEVERE, "SEVERE: AlliancePack in PACK_MAP is not available in PACK_INFO, Inconsistent Mapping (Might be NPC event Processing ISSUE): " + alliance_pack);
					throw new Exception("SEVERE: AlliancePack in PACK_MAP is not available in PACK_INFO, Inconsistent Mapping (Might be NPC event Processing ISSUE): " + alliance_pack); 

					//JOB_DETAILS.put(line_cnt, "ERROR CASE: AlliancePack in PACK_MAP is not available in PACK_INFO, Inconsistent Mapping (NPC event Processing ISSUE) : " + alliance_pack);
				}
			} else {
				//NPC not arrived yet 
				LOGGER_PVLC.log(Level.INFO, "AlliancePack is not available for Pack in PackMap TBL (NPC not Arrived Yet) for NP: " + token[1]);
				String pkmapVin = Bytes.toString(PACK_MAP_TBL.get(pkinfo_vin));
				String pkmapNP = Bytes.toString(PACK_MAP_TBL.get(pkinfo_nissanPack));

				if(pkmapVin == null || pkmapVin.isEmpty() || pkmapVin.equals(token[3])) {

					//new record or retransmitted with changed buyer or settingDate.
					LOGGER_PVLC.log(Level.INFO, "PACKMAP TBL VIN:-"+ pkmapVin + "is either empty or It equls to retransmitted VIN value in arrived event: " + token[3]);

					if(pkmapVin != null && !pkmapVin.isEmpty() && pkmapVin.equals(token[3]) && pkmapNP.equals(token[1]))
					{
						retrans_flag = true;
					}

					if (vin_idx_result != null) {
						LOGGER_PVLC.log(Level.INFO, "PACK_CHANGED for VIN: " + token[3]);
						LOGGER_PVLC.log(Level.INFO, "Removing Old Pack from VIN: " + token[3]);
						removeVINFromOldPack(token[3]);
					}
					LOGGER_PVLC.log(Level.INFO, "Attaching pack: " + token[1] + " to VIN: " + token[3]);
					//1. Updating PACK_MAP Table
					byte [] newPackMapKey = Bytes.toBytes(token[1].concat(new_UPD)); //Arrived NP+UPD
					Put NewPackMap_kv = new Put(newPackMapKey);
					NewPackMap_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(token[1])); //Arrived NP
					NewPackMap_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(token[3])); //Arrived VIN
					NewPackMap_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(token[2])); //Arrived Batter Setting Date
					NewPackMap_kv.addColumn(cf_idx, pkinfo_byte_buyer_code, Bytes.toBytes(token[4])); //setting buyer code
					NewPackMap_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));

					if(vin_idx_result != null && !retrans_flag) {
						NewPackMap_kv.addColumn(cf_idx, pkmap_old_np, VIN_IDX_TBL.get(pkinfo_nissanPack));
						NewPackMap_kv.addColumn(cf_idx, pkmap_old_np_idate, Bytes.toBytes(new_UPD));
						NewPackMap_kv.addColumn(cf_idx, pkmap_new_np, Bytes.toBytes(token[1]));
					}

					putInTable(blms_pack_info_pack_map, NewPackMap_kv);
					LOGGER_PVLC.log(Level.INFO, "End of new record for PackMap insert:" + Bytes.toString(newPackMapKey));

					//2. Updating VIN_IDX Table
					byte [] newVinKey = Bytes.toBytes(token[3].concat(new_UPD));
					Put NewVinIdx_kv = new Put(newVinKey);
					NewVinIdx_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD)); //new_UPD
					NewVinIdx_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(token[3])); //VIN column					
					NewVinIdx_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(token[2])); //Battery Setting Date
					NewVinIdx_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(token[1])); //NissanPack
					putInTable(blms_pack_info_vin_idx, NewVinIdx_kv);
					LOGGER_PVLC.log(Level.INFO, "VIN_IDX Updated for New record insert: " + Bytes.toString(newVinKey));

					JOB_DETAILS.put(line_cnt, "success");
					LOGGER_PVLC.log(Level.INFO, "Set:- JOB_DETAILS:" + String.valueOf(line_cnt) + "=success");

				} else {

					LOGGER_PVLC.log(Level.INFO, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN in PACKMAP: " + packmap_tbl_vin);
					JOB_DETAILS.put(line_cnt, "ERROR CASE: Different VIN value arrived for Nissan Pack which is already attached with another VIN, Existing VIN in PACKMAP: " + packmap_tbl_vin);

					//LOGGER_PVLC.log(Level.INFO, "ERROR CASE: Same Pack for different VIN, Pack: " + token[1]);
					//JOB_DETAILS.put(line_cnt, "ERROR CASE: Same Pack for different VIN, Pack: " + token[1]);
				} 
			} //end of NPC not arrived Yet but PVLC may retransmitted...
		} else {

			//Brand New Event arrived for PVLC 1st time., still check vin-idx tbl for any inconsistancy due to pvlu events..
			//as they are not updating PACKMAP tbl. 

			LOGGER_PVLC.log(Level.INFO, "Pack Not found in PACK_MAP, NPC or Last PVLC not arrived for arrived NP: " + token[1]);

			if (vin_idx_result != null) {
				LOGGER_PVLC.log(Level.INFO, "PACK_CHANGED for VIN: " + token[3]);
				LOGGER_PVLC.log(Level.INFO, "Removing Old Packs from VIN: " + token[3]);
				removeVINFromOldPack(token[3]);
			}

			LOGGER_PVLC.log(Level.INFO, "Attaching pack: " + token[1] + " to VIN: " + token[3]);
			//1. Updating PACK_MAP Table
			byte [] newPackMapKey = Bytes.toBytes(token[1].concat(new_UPD));
			Put NewPackMap_kv = new Put(newPackMapKey);
			NewPackMap_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(token[1])); //Arrived NP
			NewPackMap_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(token[3])); //Arrived VIN
			NewPackMap_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(token[2])); //Arrived Batter Setting Date
			NewPackMap_kv.addColumn(cf_idx, pkinfo_byte_buyer_code, Bytes.toBytes(token[4])); //setting buyer code
			NewPackMap_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));
			if(vin_idx_result != null) {
				NewPackMap_kv.addColumn(cf_idx, pkmap_old_np, VIN_IDX_TBL.get(pkinfo_nissanPack));
				NewPackMap_kv.addColumn(cf_idx, pkmap_old_np_idate, Bytes.toBytes(new_UPD));
				NewPackMap_kv.addColumn(cf_idx, pkmap_new_np, Bytes.toBytes(token[1]));
			}
			putInTable(blms_pack_info_pack_map, NewPackMap_kv);
			LOGGER_PVLC.log(Level.INFO, "End of new record for PackMap insert:" + Bytes.toString(newPackMapKey));
			//2. Updating VIN_IDX Table
			byte [] newVinKey = Bytes.toBytes(token[3].concat(new_UPD));
			Put NewVinIdx_kv = new Put(newVinKey);
			NewVinIdx_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD)); //new_UPD
			NewVinIdx_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(token[3])); //VIN column					
			NewVinIdx_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(token[2])); //Battery Setting Date
			NewVinIdx_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(token[1])); //NissanPack
			putInTable(blms_pack_info_vin_idx, NewVinIdx_kv);
			LOGGER_PVLC.log(Level.INFO, "VIN_IDX Updated for New record insert: " + Bytes.toString(newVinKey));

			JOB_DETAILS.put(line_cnt, "success");
			LOGGER_PVLC.log(Level.INFO, "Set:- JOB_DETAILS:"+String.valueOf(line_cnt) + "=success");
		}
	}
	/**
	 * To update the PACK_INFO index tables
	 * @param retrans_flag 
	 * @param PACK_INFO_TBL - Updated PACK_INFO Record
	 * @param blms_pack_info_upd_idx - Name of the UPD_IDX table
	 * @param blms_pack_info_pack_map - Name of the PACK_MAP table
	 * @param blms_pack_info_vin_idx - Name of the VIN_IDX table
	 * @throws Exception 
	 */
	private static void updatePkInfoIdxTables(boolean retrans_flag) throws Exception {
		String alliancePack = Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey));
		String nissanPack = Bytes.toString(PACK_INFO_TBL.get(pkinfo_nissanPack));
		String pvdate = Bytes.toString(PACK_INFO_TBL.get(pkinfo_pvdate));
		String buyer_code = Bytes.toString(PACK_INFO_TBL.get(pkinfo_byte_buyer_code));
		String vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));

		//1. Updating UPD_IDX Table
		LOGGER_PVLC.log(Level.INFO, "Prepare new record for:upd_idx_tbl: " + alliancePack);
		byte [] newUpdKey = Bytes.toBytes(new_UPD.concat(alliancePack));
		Put NewUpdIdx_kv = new Put(newUpdKey);
		NewUpdIdx_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));
		NewUpdIdx_kv.addColumn(cf_idx, pkinfo_alliancePk, Bytes.toBytes(alliancePack));
		putInTable(blms_pack_info_upd_idx, NewUpdIdx_kv);
		LOGGER_PVLC.log(Level.INFO, "End UPD_idx Updated for New record insert: " + Bytes.toString(newUpdKey));
		//2. Updating PACK_MAP Table
		LOGGER_PVLC.log(Level.INFO, "Prepare new record for:pack_map_tbl: " + nissanPack);
		byte [] newPackMapKey = Bytes.toBytes(nissanPack.concat(new_UPD));
		Put NewPackMap_kv = new Put(newPackMapKey);
		NewPackMap_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(nissanPack)); //Arrived NP
		NewPackMap_kv.addColumn(cf_idx, pkinfo_alliancePk, Bytes.toBytes(alliancePack)); //Arrived NP
		NewPackMap_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(vin)); //Arrived VIN
		NewPackMap_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(pvdate)); //Arrived Batter Setting Date
		NewPackMap_kv.addColumn(cf_idx, pkinfo_byte_buyer_code, Bytes.toBytes(buyer_code)); //Arrived buyer_code
		NewPackMap_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));
		//Maintaining Old Packs
		Result vin_idx_result = getLatestRecord(blms_pack_info_vin_idx, vin);
		if(vin_idx_result != null) {
			VIN_IDX_TBL = vin_idx_result.getFamilyMap(cf_idx);
			String vin_idx_np = Bytes.toString(VIN_IDX_TBL.get(pkinfo_nissanPack));
			if (vin_idx_np != null && !vin_idx_np.isEmpty() && !retrans_flag) {
				LOGGER_PVLC.log(Level.INFO, "Retaining old pack information with new pack in PackMap tbl for VIN: " + vin);
				NewPackMap_kv.addColumn(cf_idx, pkmap_old_np, VIN_IDX_TBL.get(pkinfo_nissanPack));
				NewPackMap_kv.addColumn(cf_idx, pkmap_old_np_idate, Bytes.toBytes(new_UPD));
				NewPackMap_kv.addColumn(cf_idx, pkmap_new_np, Bytes.toBytes(nissanPack));
			}
		}
		putInTable(blms_pack_info_pack_map, NewPackMap_kv);
		LOGGER_PVLC.log(Level.INFO, "End of new record for PackMap insert:" + Bytes.toString(newPackMapKey));
		//3. Updating VIN_IDX Table
		byte [] newVinKey = Bytes.toBytes(vin.concat(new_UPD));
		Put NewVinIdx_kv = new Put(newVinKey);
		NewVinIdx_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD)); //new_UPD
		NewVinIdx_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(vin)); //VIN column
		NewVinIdx_kv.addColumn(cf_idx, pkinfo_alliancePk, Bytes.toBytes(alliancePack)); //Attached AP
		NewVinIdx_kv.addColumn(cf_idx, pkinfo_pvdate, Bytes.toBytes(pvdate)); //Battery Setting Date
		NewVinIdx_kv.addColumn(cf_idx, pkinfo_nissanPack, Bytes.toBytes(nissanPack)); //NissanPack
		putInTable(blms_pack_info_vin_idx, NewVinIdx_kv);
		LOGGER_PVLC.log(Level.INFO, "VIN_IDX Updated for New record insert: " + Bytes.toString(newVinKey));
	}
	/**
	 * To update the PROBE and its index tables
	 * @param PACK_INFO_TBL - Updated PACK_INFO Record
	 * @param blms_probe - Name of the PROBE HBase table
	 * @param blms_probe_search1 - Name of the PROBE Index table - Search1
	 * @param blms_probe_search2 - Name of the PROBE Index table - Search2
	 * @param blms_probe_search3 - Name of the PROBE Index table - Search3
	 * @throws Exception 
	 */
	private static void updateProbeTables() throws Exception {
		String vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
		String pvdate = Bytes.toString(PACK_INFO_TBL.get(pkinfo_pvdate));
		LOGGER_PVLC.log(Level.INFO, "blms_probe enrichment Started for VIN: " + vin + " & PVDate: "+pvdate);
		TableName blms_probe_tbl = TableName.valueOf(hbaseNameSpace + ":" + blms_probe);
		Table blms_probe_tblConn =  conn.getTable(blms_probe_tbl);
		Scan probeScan = new Scan(); 
		LOGGER_PVLC.log(Level.INFO, "Set start Row for scan: " + vin + pvdate);
		probeScan.setStartRow(Bytes.toBytes(vin + pvdate));
		FilterList probeAllFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		probeAllFilters.addFilter(new PrefixFilter(Bytes.toBytes(vin)));
		probeScan.setFilter(probeAllFilters);
		probeScan.addFamily(cf_cmn);
		LOGGER_PVLC.log(Level.INFO, "scan object to string before execute: " + probeScan.toString());
		ResultScanner ProbeScanner = blms_probe_tblConn.getScanner(probeScan);

		//For collecting new records to insert
		List<Put> puts_probe = new ArrayList<Put>();
		List<Put> puts_probe_search1 = new ArrayList<Put>();
		List<Put> puts_probe_search2 = new ArrayList<Put>();
		List<Put> puts_probe_search3 = new ArrayList<Put>();
		//For collecting old records to delete
		List<Delete> delete_probe_search1 = new ArrayList<Delete>();
		List<Delete> delete_probe_search2 = new ArrayList<Delete>();
		List<Delete> delete_probe_search3 = new ArrayList<Delete>();
		for (Result ProbeResult = ProbeScanner.next(); ProbeResult != null; ProbeResult = ProbeScanner.next()) {
			//Updating the record in PROBE of respective VIN
			BLMS_PROBE_TBL = ProbeResult.getFamilyMap(cf_cmn);
			ProbeKey = Bytes.toString(ProbeResult.getRow());
			LOGGER_PVLC.log(Level.INFO, "Probe-RowKey for Enrichment: " + ProbeKey);
			Put NewProbe_kv = new Put(ProbeResult.getRow());
			NewProbe_kv.addColumn(cf_cmn, pkinfo_blms_id, PACK_INFO_TBL.get(pkinfo_blms_id));
			NewProbe_kv.addColumn(cf_cmn, pkinfo_alliancePk, PACK_INFO_TBL.get(pkinfo_alliancePk));
			NewProbe_kv.addColumn(cf_cmn, pkinfo_nissanPack, PACK_INFO_TBL.get(pkinfo_nissanPack));
			NewProbe_kv.addColumn(cf_cmn, pkinfo_vin, Bytes.toBytes(vin)); //VIN
			NewProbe_kv.addColumn(cf_cmn, pkinfo_PackDateKey, PACK_INFO_TBL.get(pkinfo_PackDateKey));
			NewProbe_kv.addColumn(cf_cmn, pkinfo_pvdate, Bytes.toBytes(pvdate)); //Battery Setting Date
			NewProbe_kv.addColumn(cf_cmn, pkinfo_upd, Bytes.toBytes(new_UPD));//new UPD
			puts_probe.add(NewProbe_kv);

			//Updating PROBE Index tables
			String new_probe_APack = Bytes.toString(PACK_INFO_TBL.get(pkinfo_alliancePk));
			String new_probe_ts = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_Timestamp));
			//Updating row in Search1
			String newSearch1Key = new_probe_APack.concat(new_UPD).concat(new_probe_ts);
			Put NewProbe_Search1_kv = new Put(Bytes.toBytes(newSearch1Key));
			NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_vin, Bytes.toBytes(vin));
			NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_Timestamp, Bytes.toBytes(new_probe_ts));
			NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_upd, Bytes.toBytes(new_UPD));
			NewProbe_Search1_kv.addColumn(cf_idx,pkinfo_alliancePk, Bytes.toBytes(new_probe_APack));
			puts_probe_search1.add(NewProbe_Search1_kv);
			//Updating row in Search2
			String newSearch2Key = new_UPD.concat(new_probe_APack).concat(new_probe_ts);
			Put NewProbe_Search2_kv = new Put(Bytes.toBytes(newSearch2Key));
			NewProbe_Search2_kv.addColumn(cf_idx, pkinfo_vin, Bytes.toBytes(vin));
			NewProbe_Search2_kv.addColumn(cf_idx, pkinfo_Timestamp, Bytes.toBytes(new_probe_ts));
			NewProbe_Search2_kv.addColumn(cf_idx, pkinfo_upd, Bytes.toBytes(new_UPD));
			NewProbe_Search2_kv.addColumn(cf_idx, pkinfo_alliancePk, Bytes.toBytes(new_probe_APack));			
			puts_probe_search2.add(NewProbe_Search2_kv);
			//Updating row in Search3
			String newSearch3Key = new_UPD.concat(vin).concat(new_probe_ts);
			Put NewProbe_Search3_kv = new Put(Bytes.toBytes(newSearch3Key));
			NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_vin,Bytes.toBytes(vin));
			NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_Timestamp,Bytes.toBytes(new_probe_ts));
			NewProbe_Search3_kv.addColumn(cf_idx,pkinfo_upd,Bytes.toBytes(new_UPD));
			puts_probe_search3.add(NewProbe_Search3_kv);



			//Deleting old records from PROBE Index tables
			String old_probe_vin = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_orignalKey));
			String old_probe_APack = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_alliancePk));
			String old_probe_upd = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_upd));
			String old_probe_ts = Bytes.toString(BLMS_PROBE_TBL.get(pkinfo_Timestamp));

			//String oldSearch1Key;
			//String oldSearch2Key;
			//boolean ap_exist=false;

			//Search1:ap+upd+ts
			if(old_probe_APack!=null && !old_probe_APack.isEmpty())
			{
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

		LOGGER_PVLC.log(Level.INFO, "blms_probe tbl Insert Started for the event Line Number: " + line_cnt);
		putInTable(blms_probe, puts_probe);
		LOGGER_PVLC.log(Level.INFO, "blms_probe tbl Insert Finished for the event Line Number: " + line_cnt);
		//LOGGER_PVLC.log(Level.INFO, "PROBE_TBL-PROBE KEY ENRICHED..: " + ProbeKey);
		//LOGGER_PVLC.log(Level.INFO, "Starting Index Tbls updates for PROBE KEY: " + ProbeKey);

		LOGGER_PVLC.log(Level.INFO, "blms_probe_search1 tbl Insert Started for the event Line Number: " + line_cnt);
		putInTable(blms_probe_search1, puts_probe_search1);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search1 tbl Insert Finished for the event Line Number: " + line_cnt);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search1 tbl delete for Old Keys, Started for the event Line Number: " + line_cnt);
		deleteFromTable(blms_probe_search1, delete_probe_search1);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search1 tbl delete for Old Keys, Finished for the event Line Number: " + line_cnt);
		//LOGGER_PVLC.log(Level.INFO, "UPDATE IDX:newSearch1Key: " + newSearch1Key);
		//LOGGER_PVLC.log(Level.INFO, "DELETE IDX:oldSearch1Key: " + oldSearch1Key);

		LOGGER_PVLC.log(Level.INFO, "blms_probe_search2 tbl Insert Started for the event Line Number: " + line_cnt);
		putInTable(blms_probe_search2, puts_probe_search2);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search2 tbl Insert Finished for the event Line Number: " + line_cnt);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search2 tbl old key delete, Started for the event Line Number: " + line_cnt);
		deleteFromTable(blms_probe_search2, delete_probe_search2);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search2 tbl old key delete, Finished for the event Line Number: " + line_cnt);
		//LOGGER_PVLC.log(Level.INFO, "UPDATE IDX:newSearch2Key: " + newSearch2Key);
		//LOGGER_PVLC.log(Level.INFO, "DELETE IDX:oldSearch2Key: " + oldSearch2Key);

		LOGGER_PVLC.log(Level.INFO, "blms_probe_search3 tbl Insert Started for the event Line Number: " + line_cnt);
		putInTable(blms_probe_search3, puts_probe_search3);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search3 tbl Insert finished for the event Line Number: " + line_cnt);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search3 tbl delete for old key, Started for the event Line Number: " + line_cnt);
		deleteFromTable(blms_probe_search3, delete_probe_search3);
		LOGGER_PVLC.log(Level.INFO, "blms_probe_search3 tbl delete for old key, Finished for the event Line Number: " + line_cnt);
		//LOGGER_PVLC.log(Level.INFO, "UPDATE IDX:newSearch3Key: " + newSearch3Key);
		//LOGGER_PVLC.log(Level.INFO, "DELETE IDX:oldSearch3Key: " + oldSearch3Key);
	}
	/**
	 * To process the job_details records
	 * @param line - PVLC Input Record
	 * @param outPutStream - HDFS Output Streamer to write the contents
	 * @throws IOException if any issue with writing the contents
	 */
	private static void ProcessJDRecords(String line, FSDataOutputStream outPutStream ) throws IOException {
		String record = JOB_DETAILS.get(line_cnt);
		if (record != null && !record.isEmpty()) {
			if (record.compareTo("success") == 0) {
				LOGGER_PVLC.log(Level.INFO, "SUCCESS, Event Line number: " + line_cnt);
			}else {
				LOGGER_PVLC.log(Level.INFO, "ERROR, Event Line number: " + line_cnt);
			}	
			LOGGER_PVLC.log(Level.INFO, "Writing to CSV: ");
			WriteToCsvErrFile(line, outPutStream);
			LOGGER_PVLC.log(Level.INFO, "Successfuly inserted into WriteToCsvErrFile(), for Event line number: " + line_cnt);
		}
	}
	/**
	 * To get the HDFS configuration
	 * @param defaultFS - Hadoop File System
	 * @param hadoopSecurityAuthentication - Security Authentication
	 * @return HDFS Configuration
	 * @throws IOException if any issue HDFS connection
	 */
	private static Configuration getHDFSConf() throws IOException {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", defaultFS);
		config.set("hadoop.security.authentication", hadoopSecurityAuthentication);
		UserGroupInformation.setConfiguration(config);
		UserGroupInformation.loginUserFromSubject(null);
		return config;
	}
	/**
	 * To get the HBase configuration
	 * @param hbaseZookeeperPort
	 * @param hbaseZookeeperQuorum1
	 * @param hbaseZnodeParent
	 * @param hbaseMasterPort
	 * @param hbaseMaster
	 * @param hbaseRpcTimeout
	 * @param hbaseClientScannerTimeout
	 * @param hbaseCellsScannedPerHeartBeatCheck
	 * @param hbaseClientOperationTimeout
	 * @return HBase Configuration 
	 * @throws IOException if any issue with HBase connection
	 */
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
	/**
	 * To write the success and error records to CSV file
	 * @param line - PVLC input record
	 * @param outPutStream - HDFS Output Streamer to write the contents
	 * @throws IOException if any issue with writing the contents
	 */
	private static void WriteToCsvErrFile(String line, FSDataOutputStream outPutStream) throws IOException {
		String JDline = line + "\t" + JOB_DETAILS.get(line_cnt);
		outPutStream.writeUTF(JDline);
		outPutStream.writeUTF("\n");
		outPutStream.flush();
	}
	/**
	 * To allocate the resources (Logger and HBase Connection)
	 * @param Log File Name
	 * @param HBase Configuration
	 * @throws IOException 
	 * @throws SecurityException 
	 */
	/*	public final static void getResources() {
		SimpleFormatter formatter = new SimpleFormatter();
		try {
			fh_pvlc = new FileHandler(PVLC_UPDATES_LOG + "_" + new_UPD.replaceAll("[^0-9]", ""));
			fh_pvlc.setFormatter(formatter); 
			LOGGER_PVLC.addHandler(fh_pvlc);
			LOGGER_PVLC.setUseParentHandlers(false);
			ExecutorService executor = Executors.newFixedThreadPool(1);
			conn = ConnectionFactory.createConnection(getHBASEConf(), executor);
			LOGGER_PVLC.log(Level.FINE, "PVLC Execution Started !!!");
		} catch (SecurityException | IOException e) {
			e.printStackTrace();
			releaseResources();
		}
	} */

	public final static void getResources(String WF_ID) throws SecurityException, IOException {
		SimpleFormatter formatter = new SimpleFormatter();
		//fh_pvlc = new FileHandler(PVLC_UPDATES_LOG + "_" + new_UPD.replaceAll("[^0-9]", ""));
		fh_pvlc = new FileHandler(PVLC_UPDATES_LOG + "_" + WF_ID.replaceAll("[^0-9]", ""));
		fh_pvlc.setFormatter(formatter); 
		LOGGER_PVLC.addHandler(fh_pvlc);
		LOGGER_PVLC.setUseParentHandlers(false);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		conn = ConnectionFactory.createConnection(getHBASEConf(), executor);
		//LOGGER_PVLC.log(Level.FINE, "PVLC Execution Started !!!");
	}

	/**
	 * To release the allocated resources
	 * @throws Exception 
	 */
	private static void releaseResources() throws Exception {
		try {

			if (br != null)
			{
				br.close();
				LOGGER_PVLC.log(Level.INFO,"br.close() executed...");
			}
			if (outPutStream != null) 
			{
				outPutStream.close();
				LOGGER_PVLC.log(Level.INFO,"outPutStream.close() executed...");
			}
			if (fs != null)
			{
				fs.close();
				LOGGER_PVLC.log(Level.INFO,"fs.close() executed...");
			}
			if (conn != null)
			{
				conn.close();
				LOGGER_PVLC.log(Level.INFO,"conn.close() executed...");
			}
			
			if (fh_pvlc != null)
			{
				fh_pvlc.close();
				LOGGER_PVLC.log(Level.INFO,"fh_pvlc.close() executed...");
			}
		}catch(Exception e1) {
			e1.printStackTrace();
			throw new Exception("SEVERE:Exception in releaseResources()...."+e1.getMessage());
		}
	}
	/**
	 * Entry point of the program, calls applyUpdates() to process the PVLC records
	 * @param args[0] - PVLC HDFS Input Path
	 * @param args[1] - Update Date
	 * @throws Exception 
	 */
	public static void main(String args[]) throws Exception {
		try {
			if(args.length != 3) {
				throw new IOException("SEVERE:PVLC JAVA process Requires[filePath, newUPD, V_READ_DATE], number of passed args not matching with expected..."); 
			}else {
				filePath = new Path(args[0]);
				new_UPD = args[1];
				V_READ_DATE=args[2]+" +0000";
				jdCSVFilePath = new Path(args[0] + "_JOBDETAILS");

				getResources(args[2]); //Initializing the LogFile for PVLC Process
				printInitialValues();

				LOGGER_PVLC.log(Level.INFO, "PVLC Log file initiated for the day: " + new_UPD);
				LOGGER_PVLC.log(Level.INFO, "PVLC Input Path: " + filePath);
				LOGGER_PVLC.log(Level.INFO, "Input JOB_DETAILS CSV file path: " + jdCSVFilePath);
				LOGGER_PVLC.log(Level.INFO, "Input new UPD: " + args[1]);		

				applyUpdates(); //To process all PVLC events

				//Release Resources held by this Process.
				LOGGER_PVLC.log(Level.INFO,"Job finished..Calling releaseResources() as normal call..");
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

