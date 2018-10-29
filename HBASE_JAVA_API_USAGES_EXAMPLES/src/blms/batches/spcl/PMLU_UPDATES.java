/*
 * compile with
 * javac -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PMLU_UPDATES.java
 * nohup java -classpath `hadoop classpath`:`hbase classpath` blms/batches/spcl/PMLU_UPDATES 'hdfs://hahdfsqa/user/asp20571bdp/BLMB0102/PMLU/input/111111_20200201130000/VALID_PMLU_INPUT_FOR_JAVA/part-v009-o000-r-00000' '2020-02-01 13:00:00.000' '20200201130000' > PMLU.log 2>&1 &
 * 
 */
package blms.batches.spcl;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
public class PMLU_UPDATES {
	private static Properties configProp = new Properties();
	static Map<byte[], byte[]> PACK_INFO_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> BLMS_PROBE_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_UPD_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_INFO_VIN_IDX_TBL = new HashMap<byte[], byte[]>();
	static Map<byte[], byte[]> PACK_MAP_TBL = new HashMap<byte[], byte[]>();
	static Map<Integer, String> JOB_DETAILS = new HashMap<Integer, String>(); //Collect the success or error case of each pvlu record

	static int line_cnt = 0;

	private static String JOB_UPD; 
	private static String V_READ_DATE;
	private static String row_upd;
	//private static long row_now_ts;

	private static Path filePath ;
	private static Path jdCSVFilePath ;

	private static FileHandler fh_pmlu;

	private  static String pmlu_event_id;
	private  static String event_remove_flag;
	private  static String event_set_flag;

	private  static byte[] cf_pk, cf_idx, cf_cp, pkinfo_Battery_Pack_Completion_Test_Date, pkinfo_blms_id, pkinfo_vin, pkinfo_upd ,pkinfo_orignalKey ,pkinfo_alliancePk,pkinfo_Timestamp, pkinfo_event_id, PkInfo_FLD_q, PkInfo_FLD_r, PkInfo_FLD_s, PkInfo_FLD_t, PkInfo_FLD_u, PkInfo_FLD_v, PkInfo_FLD_w, PkInfo_FLD_x, PkInfo_FLD_h, PkInfo_FLD_i, PkInfo_FLD_j;
	private  static byte[] CP_V_COL_001, CP_V_COL_002;

	private  static String PkInfo_VAL_s, PkInfo_VAL_t, PkInfo_VAL_v, PkInfo_VAL_w, PkInfo_VAL_x, PkInfo_VAL_h, PkInfo_VAL_i, PkInfo_VAL_j;

	private  static String hbaseNameSpace;
	private  static String blms_pack_info, blms_pack_info_upd_idx, blms_pack_info_vin_idx, blms_cache_processed, blms_mc;
	private  static String defaultFS, hbaseZookeeperPort, hbaseZookeeperQuorum1, hbaseZnodeParent, hbaseMasterPort, hbaseMaster, hbaseRpcTimeout, hbaseClientScannerTimeout, hbaseCellsScannedPerHeartBeatCheck, hbaseClientOperationTimeout, hadoopSecurityAuthentication;
	private  static String PMLU_UPDATES_LOG;

	private static Logger LOGGER_EVENT;

	private  static Table blms_pack_info_tblConn, blms_mc_tblConn,blms_pack_info_vin_idx_tblConn, TblBlmsConn, blms_probe_tblConn;
	private  static ResultScanner mc_scanner, scanner, vin_scanner, ProbeScanner, vinIdxScanner;
	private  static FileSystem fs;
	private  static FileSystem hdfsOut;
	private  static FSDataOutputStream outPutStream;
	private  static Configuration conf;
	private  static BufferedReader br;
	static Connection conn;

	private static final String PropertyFileName ="/home/asp20571bdp/DEV/BDP_BATCHES/COMMON_UTILS/PROPERTIES/conf_blmb0102.properties";
	static {
		//InputStream in = PMLU_UPDATES.class.getClassLoader().getResourceAsStream("blms/batches/spcl/conf_blmb0102.properties");
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
			pmlu_event_id = configProp.getProperty("pmlu_event_id");
			event_remove_flag = configProp.getProperty("event_remove_flag");
			event_set_flag = configProp.getProperty("event_set_flag");
			cf_pk = Bytes.toBytes(configProp.getProperty("cf_pk"));
			cf_idx = Bytes.toBytes(configProp.getProperty("cf_idx"));
			cf_cp = Bytes.toBytes(configProp.getProperty("cf_cp"));
			pkinfo_Battery_Pack_Completion_Test_Date = Bytes.toBytes(configProp.getProperty("pkinfo_Battery_Pack_Completion_Test_Date"));
			pkinfo_blms_id = Bytes.toBytes(configProp.getProperty("pkinfo_blms_id"));
			pkinfo_vin = Bytes.toBytes(configProp.getProperty("pkinfo_vin"));
			pkinfo_upd = Bytes.toBytes(configProp.getProperty("pkinfo_upd"));
			pkinfo_orignalKey = Bytes.toBytes(configProp.getProperty("pkinfo_orignalKey"));
			pkinfo_alliancePk = Bytes.toBytes(configProp.getProperty("pkinfo_alliancePk"));
			pkinfo_Timestamp = Bytes.toBytes(configProp.getProperty("pkinfo_Timestamp"));
			pkinfo_event_id = Bytes.toBytes(configProp.getProperty("pkinfo_event_id"));
			CP_V_COL_001 = Bytes.toBytes(configProp.getProperty("CP_V_COL_001"));
			CP_V_COL_002 = Bytes.toBytes(configProp.getProperty("CP_V_COL_002"));
			hbaseNameSpace = configProp.getProperty("hbaseNameSpace");
			blms_pack_info = configProp.getProperty("blms_pack_info");
			blms_pack_info_upd_idx = configProp.getProperty("blms_pack_info_upd_idx");
			blms_pack_info_vin_idx = configProp.getProperty("blms_pack_info_vin_idx");
			blms_cache_processed = configProp.getProperty("blms_cache_processed");
			blms_mc = configProp.getProperty("blms_mc");
			PMLU_UPDATES_LOG = configProp.getProperty("PMLU_UPDATES_LOG");

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

			LOGGER_EVENT = Logger.getLogger(PMLU_UPDATES.class.getName());
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
		LOGGER_EVENT.log(Level.INFO,"PMLU_UPDATES_LOG: "+PMLU_UPDATES_LOG);
		LOGGER_EVENT.log(Level.INFO,"pmlu_event_id: "+pmlu_event_id);
		LOGGER_EVENT.log(Level.INFO,"event_remove_flag: "+event_remove_flag);
		LOGGER_EVENT.log(Level.INFO,"event_set_flag: "+event_set_flag);
		LOGGER_EVENT.log(Level.INFO,"cf_pk: "+Bytes.toString(cf_pk));
		LOGGER_EVENT.log(Level.INFO,"cf_idx: "+Bytes.toString(cf_idx));
		LOGGER_EVENT.log(Level.INFO,"cf_cp: "+Bytes.toString(cf_cp));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_blms_id: "+Bytes.toString(pkinfo_blms_id));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_vin: "+Bytes.toString(pkinfo_vin));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_upd: "+Bytes.toString(pkinfo_upd));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_orignalKey: "+Bytes.toString(pkinfo_orignalKey));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_alliancePk: "+Bytes.toString(pkinfo_alliancePk));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_Timestamp: "+Bytes.toString(pkinfo_Timestamp));
		LOGGER_EVENT.log(Level.INFO,"pkinfo_event_id: "+Bytes.toString(pkinfo_event_id));
		LOGGER_EVENT.log(Level.INFO,"CP_V_COL_001: "+Bytes.toString(CP_V_COL_001));
		LOGGER_EVENT.log(Level.INFO,"CP_V_COL_002: "+Bytes.toString(CP_V_COL_002));
		LOGGER_EVENT.log(Level.INFO,"hbaseNameSpace: "+hbaseNameSpace);
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


	public static void putInTable(String blms_tbl, Put PutRowList) throws Exception{
		try {
			TblBlmsConn = getTable(blms_tbl);

			RowMutations mutations = new RowMutations(PutRowList.getRow());
			mutations.add(PutRowList);
			TblBlmsConn.mutateRow(mutations);
			//PutRowList.setDurability(Durability.FSYNC_WAL);
			//TblBlmsConn.put(PutRowList);

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
			throw new Exception("SEVERE:Exception in putInTable().."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable().."+e.getMessage());
		}
	}//end putTable()

	public static void putInTable(String blms_tbl, List<Put> PutRowList) throws Exception {
		try {
			TblBlmsConn = getTable(blms_tbl);
			TblBlmsConn.put(PutRowList);
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
			throw new Exception("SEVERE:Exception in putInTable().."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in putInTable().."+e.getMessage());
		}
	}//end putTable()
	public static void deleteFromTable(String DeleteFromTable, List<Delete> DeleteRowList) throws Exception{
		try {
			TblBlmsConn = getTable(DeleteFromTable);
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
			throw new Exception("SEVERE:Exception in deleteFromTable().."+e.getMessage());
		}catch(Exception e){
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in deleteFromTable().."+e.getMessage());
		}
	}//end deleteFromTable

	public static void getUPD() {
		Date dNow = new Date( );
		SimpleDateFormat ft =
				new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.S");
		row_upd = ft.format(dNow);
		//row_now_ts = System.currentTimeMillis() + line_cnt;
		//LOGGER_EVENT.log(Level.INFO,"row_now_ts="+row_now_ts);
		LOGGER_EVENT.log(Level.INFO,"row_upd= "+row_upd);
	}

	public static void applyUpdates() throws Exception{
		try {	
			conf = getHDFSConf();
			fs = FileSystem.get(conf);
			LOGGER_EVENT.log(Level.INFO,"HDFS file path for pmlu input:-"+filePath);
			if(!fs.exists(filePath)) {
				LOGGER_EVENT.log(Level.SEVERE,"INVALID ARGUMENTS: Path doest not exist..");
				throw new Exception("SEVERE:INVALID ARGUMENTS: Path doest not exist..");
				//System.exit(1); //Exit.
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
						getUPD();
						/*  Pack_Module_Linkage_Update      295B03NA7A BD1183T000091        20180328145800 +0000    1       295B93NA0BTBD917BM0002862       20180328235800 +0900
    			20180420220724+0000     2018-04-21 07:07:24.125 pack_module_linkage_update_list_201804_000.csv  
    			hdfs:/common/20006_SaW/10838/trn_lthm_bttr_pckg_md_updt/abinitio_process_time=20180420/trn_lthm_bttr_pckg_md_updt_20180420172045889.tsv
    			trn_lthm_bttr_pckg_md_updt_20180420172045889.tsv        20180420172749  20180420220724
						 */
						//token[10] --> eventfileName.-->trn_lthm_bttr_pckg_md_updt_20180420172045889.tsv

						String token[] =  line.split("\t");
						LOGGER_EVENT.log(Level.INFO,"token lenght:-"+token.length);
						LOGGER_EVENT.log(Level.INFO,"input lineCnt taken for processing: "+line_cnt);
						LOGGER_EVENT.log(Level.INFO,"Input Event Record for Processing: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]+" InputFilename:- "+token[10]);
						if(token[3].equals(event_remove_flag) && token[0].equals(pmlu_event_id))
						{
							LOGGER_EVENT.log(Level.INFO,"Removing Module:-"+token[4]+" Pack:-"+token[1]);
							LOGGER_EVENT.log(Level.INFO,"Calling pmluRemoveApply()");
							pmluRemoveApply(token);
							LOGGER_EVENT.log(Level.INFO,"pmluRemoveApply() Process:Done for Event Line number: "+line_cnt);

						}else if(token[3].equals(event_set_flag) && token[0].equals(pmlu_event_id))
						{
							LOGGER_EVENT.log(Level.INFO,"Attaching Module:-"+token[4]+" Pack:-"+token[1]);
							LOGGER_EVENT.log(Level.INFO,"Calling pmluSetApply()");
							pmluSetApply(token);
							LOGGER_EVENT.log(Level.INFO,"pmluSetApply() Process:Done for Event Line number: "+line_cnt);
						}//end pmlu set

						ProcessErrorEvent(line);
					}//while end
				}//End of file list
			}//end of else file exist
		}catch(Exception e) {	//LOGGER_EVENT.log(Level.SEVERE,"Error In getHDFSConf(), failed to read the hdfs filesystem using provided conf..");
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in applyUpdates().."+e.getMessage());
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
		String JDline = line+"\t"+JOB_DETAILS.get(line_cnt)+"\t";
		try {
			outPutStream.writeUTF(JDline);
			outPutStream.writeUTF("\n");
			outPutStream.flush();
			LOGGER_EVENT.log(Level.INFO,"Successfuly inserted into CSV, for Event line number: "+line_cnt);
		} catch (Exception e) {
			//LOGGER_EVENT.log(Level.SEVERE,"Error In WriteToCsvErrFile(), failed to Write into hdfs filesystem.."+jdCSVFilePath);
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in WriteToCsvErrFile().."+e.getMessage());
		}
	}

	private static void pmluSetApply(String[] token) throws Exception{
		/* 1. blms_pack, AP exist ?
		 * 2. if true, Check Attaching Module exist in mc tbl
		 * 3. if true, Attach the module and dt for this pack & create new record for 
		 *  														a. pkinfo : take all other existing field as is 
		 * 															b. upd_idx.
		 * 															c. vin_idx.
		 * 															d. insert in CP tbl.
		 * else AddInto errorCSV
		 * 		
		 */
		try {
			LOGGER_EVENT.log(Level.INFO,"Inside Of pmluSetApply() for Attaching ModuleID: "+token[4]+" & AP: "+token[1]);
/*			blms_mc_tblConn = getTable(blms_mc);
			Scan mc_scan = new Scan(); 
			LOGGER_EVENT.info("Set start Row for scan: "+token[4]);
			mc_scan.setStartRow(Bytes.toBytes(token[4])); //token[4] <- a_mod
			FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4]))); //<- a_mod
			mc_scan.setFilter(allFilters);
			mc_scan.setMaxResultSize(1);
			LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+mc_scan.toString());
			mc_scanner = blms_mc_tblConn.getScanner(mc_scan);
			Result mc_result = mc_scanner.next();
*/			
			Result mc_result =  getLatestRecord(blms_mc,token[4]);
			if(mc_result != null)
			{
				LOGGER_EVENT.log(Level.INFO,"Arrived A_MOD is present in MC tbl, Arrived A_MOD RowKey: "+Bytes.toString(mc_result.getRow())); 
				LOGGER_EVENT.log(Level.INFO,"Check: Arrived Pack is Present in PackInfo Tbl: "+token[1]);
/*
				blms_pack_info_tblConn = getTable(blms_pack_info);
				Scan scan = new Scan(); 

				FilterList allFilters_pkinfo = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				allFilters_pkinfo.addFilter(new PrefixFilter(Bytes.toBytes(token[1]))); //<- AP
				scan.setFilter(allFilters_pkinfo);
				scan.setReversed(true); // read the latest available key and values for this AP
				scan.setMaxResultSize(1);
				LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+scan.toString());
				scanner = blms_pack_info_tblConn.getScanner(scan);
				Result result = scanner.next();
*/
				Result result =  getLatestRecord(blms_pack_info,token[1]);
				if(result != null)
				{
					PACK_INFO_TBL = result.getFamilyMap(cf_pk);
					LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
					LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
					LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
					LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get( pkinfo_orignalKey)));
					String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
					String cond2_a_mod = token[4]; //<- A_Mod
					String cond3_a_mod_dt = token[2]; //<- A_Mod_dt
					byte [] a_mod = Bytes.toBytes(cond2_a_mod);
					byte [] a_mod_dt = Bytes.toBytes(cond3_a_mod_dt);

					String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
					LOGGER_EVENT.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
					LOGGER_EVENT.log(Level.INFO,"cond2_a_mod: "+cond2_a_mod);

					if(cond1_blms_pack_id !=null && !cond1_blms_pack_id.isEmpty())	
					{
						LOGGER_EVENT.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
						//blms id present, arrived alliance pack present.: true
						LOGGER_EVENT.log(Level.INFO,"Next check, Vacant position for Arrived a_mod: "+cond2_a_mod+"to attach for this Pack in PKinfo tbl: "+token[1]);
						//Iterate over ModList and check the vacant position till last valid position.:
						//Modify from below onwards....
						boolean flag = false;
						for(int i=3;i<=97;i=i+2)
						{   byte [] i_byte = Bytes.toBytes(String.valueOf(i));
						byte [] i_plus_byte = Bytes.toBytes(String.valueOf(i+1));
						String pk_mod_pos = Bytes.toString(PACK_INFO_TBL.get(i_byte));
						if(pk_mod_pos ==null || pk_mod_pos.isEmpty()) {
							LOGGER_EVENT.log(Level.INFO,"Vacant Position Found to attach A_MOD At: "+i);
							LOGGER_EVENT.log(Level.INFO,"Attaching ....");
							PACK_INFO_TBL.put(i_byte, a_mod);
							PACK_INFO_TBL.put(i_plus_byte, a_mod_dt);
							PACK_INFO_TBL.put(pkinfo_Battery_Pack_Completion_Test_Date, a_mod_dt);
							PACK_INFO_TBL.put(PkInfo_FLD_r, Bytes.toBytes(token[10])); //Set inputEventFilename as pmlu event registering, AMO cols
							PACK_INFO_TBL.put(PkInfo_FLD_q, Bytes.toBytes(V_READ_DATE)); //Set ReadDate for this CSV registration, AMO cols
							flag = true;
							break;
						}
						}
						if (!flag) {
							LOGGER_EVENT.log(Level.INFO,"ERROR CASE: A-MOD-"+token[4]+" doesn't have vacant position in module list to attach for the Pack: "+token[1]);
							JOB_DETAILS.put(line_cnt,"ERROR CASE: A-MOD-"+token[4]+" doesn't have vacant position in module list to attach for the Pack: "+token[1]);
						}else {
							//Update the Packinfo, UpdIdx, vinIdx tbl for new rowkey-values
							LOGGER_EVENT.log(Level.INFO,"Preparing Index Updates for VIN_IDX_TBL: "+cond2_pkinfo_vin);
							//Prepare new record for:vin_idx_tbl: Bring all latest values as is and update the rowkey.
/*							blms_pack_info_vin_idx_tblConn = getTable(blms_pack_info_vin_idx);
							Scan vin_scan = new Scan(); 
							FilterList allFilters_vin = new FilterList(FilterList.Operator.MUST_PASS_ALL);
							allFilters_vin.addFilter(new PrefixFilter(PACK_INFO_TBL.get(pkinfo_vin))); //<- vin key
							vin_scan.setFilter(allFilters_vin);
							vin_scan.setReversed(true); // read the latest available key and values for this AP:VIN
							vin_scan.setMaxResultSize(1);
							LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+vin_scan.toString());
							vin_scanner = blms_pack_info_vin_idx_tblConn.getScanner(vin_scan);
							Result vin_result = vin_scanner.next();
*/
							Result vin_result =  getLatestRecord(blms_pack_info_vin_idx,Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
							
							if(vin_result != null)
							{   
								VIN_IDX_TBL = vin_result.getFamilyMap(cf_idx);
								String vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));

								byte [] newVinIdxKey = Bytes.toBytes(vin.concat(row_upd));
								Put NewVinIdx_kv = new Put(newVinIdxKey); 
								VIN_IDX_TBL.put(pkinfo_upd,Bytes.toBytes(row_upd));
								LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_upd)+": "+row_upd);		    	 			
								for (Entry<byte[], byte[]> entry : VIN_IDX_TBL.entrySet()) {
									byte[] key = entry.getKey();
									byte[] value = entry.getValue();
									LOGGER_EVENT.log(Level.INFO,Bytes.toString(key)+":"+Bytes.toString(value));
									//NewVinIdx_kv.addColumn(cf_idx,key,row_now_ts, value);
									NewVinIdx_kv.addColumn(cf_idx,key, value);
								}

								LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Started for the event Line Number: "+line_cnt);
								putInTable(blms_pack_info_vin_idx,NewVinIdx_kv);

								LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Finished for the event Line Number: "+line_cnt);
								LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx Updated for New record insert.."+Bytes.toString(newVinIdxKey));
							}else {//no PVLC or PVLU so far for this Pack
								LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl doesn't have index for arrived Pack.."+token[1]);
							}
							//Prepare new record for:upd_idx_tbl
							String AP = Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey));
							LOGGER_EVENT.log(Level.INFO,"Preparing Index Updates for:upd_idx_tbl..");
							byte [] newUpdIdxKey = Bytes.toBytes(row_upd.concat(AP));
							Put NewUpdIdx_kv = new Put(newUpdIdxKey); 
							//NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd, row_now_ts, Bytes.toBytes(row_upd));
							//NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk, row_now_ts, PACK_INFO_TBL.get(pkinfo_orignalKey));
							NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd,  Bytes.toBytes(row_upd));
							NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk,  PACK_INFO_TBL.get(pkinfo_orignalKey));
							LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_upd)+":"+row_upd);
							LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_alliancePk)+":"+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info_upd_idx,NewUpdIdx_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Finished for the event Line Number: "+line_cnt);
							LOGGER_EVENT.log(Level.INFO,"End UPD_idx Updated for New record insert.."+Bytes.toString(newUpdIdxKey));

							//Preparing new record for packInfo:
							LOGGER_EVENT.log(Level.INFO,"Preparing new record for packInfo..");
							PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(row_upd));

							byte [] newPackInfoKey = Bytes.toBytes(AP.concat(row_upd));
							Put NewPackInfo_kv = new Put(newPackInfoKey); 
							for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
								byte[] key = entry.getKey();
								byte[] value = entry.getValue();
								LOGGER_EVENT.log(Level.INFO,Bytes.toString(key)+":"+Bytes.toString(value));
								//NewPackInfo_kv.addColumn(cf_pk,key, row_now_ts, value);
								NewPackInfo_kv.addColumn(cf_pk,key, value);
							}

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info,NewPackInfo_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Finished for the event Line Number: "+line_cnt);
							//end of new record for packInfo insert 
							LOGGER_EVENT.log(Level.INFO,"End of new record for packInfo insert.."+Bytes.toString(newPackInfoKey));

							//@@Insert successful event in cash_processed_tbl 
							LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert Started for the event Line Number: "+line_cnt);
							putIntoCashProcessed(token);
							LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert finished for the event Line Number: "+line_cnt);

							JOB_DETAILS.put(line_cnt,"success");
							LOGGER_EVENT.log(Level.INFO,"Set:- JOB_DETAILS:"+String.valueOf(line_cnt)+"=success");
						}//set applied happened on packinfo
					}else { //blmsPack check in packinfo
						LOGGER_EVENT.log(Level.INFO,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for Attaching Pack.."+token[1]);
						JOB_DETAILS.put(line_cnt,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for Attaching Pack.."+token[1]);
					}
				}else { //packinfo result check
					LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived Pack is not Present in PackInfo Table.."+token[1]);
					JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived Pack is not Present in PackInfo Table.."+token[1]);
				}
			}else {//mc tbl check
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived A_MOD is not present in MC tbl, Arrived A_MOD: "+token[4]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived A_MOD is not present in MC tbl, Arrived A_MOD: "+token[4]);
			}
		}catch(Exception e) {
			//LOGGER_EVENT.log(Level.SEVERE,"Error in pmluSetApply()..");
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in pmluSetApply().."+e.getMessage());
		}
	} //end of pmluSetApply

	private static Table getTable(String tblName) throws IOException {
		TableName blms_tbl = TableName.valueOf(hbaseNameSpace+":"+tblName);
		Table blms_tblConn = conn.getTable(blms_tbl);
		return blms_tblConn;
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
	

	private static void pmluRemoveApply(String[] token) throws Exception{
		/*
		 * 1. blms_pack, AP exist ?
		 * 2. if true, Check Removing Module exist in mc tbl
		 * 3. if true, Check Removing Module is really Attached to this Pack
		 *  If true, Detach the module and dt for this pack & create new record for 
		 *  														a. pkinfo : take all other existing feild as is 
		 * 															b. upd_idx.
		 * 															c. vin_idx.
		 * 															d. insert in CP tbl
		 * else AddInto errorCSV
		 * 		
		 */
		try {
			LOGGER_EVENT.log(Level.INFO,"Inside Of pmluRemoveApply() for removing ModuleID: "+token[4]+" & AP: "+token[1]);
			/*//Sakaguchi san, asked to remove this check during integration testing..
			blms_mc_tblConn = getTable(blms_mc);
			Scan mc_scan = new Scan(); 
			LOGGER_EVENT.info("Set start Row for scan: "+token[4]);
			mc_scan.setStartRow(Bytes.toBytes(token[4])); //token[4] <- r_mod
			FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			allFilters.addFilter(new PrefixFilter(Bytes.toBytes(token[4]))); //<- r_mod
			mc_scan.setFilter(allFilters);
			mc_scan.setMaxResultSize(1);
			LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+mc_scan.toString());
			mc_scanner = blms_mc_tblConn.getScanner(mc_scan);
			Result mc_result = mc_scanner.next();

			if(mc_result != null)
			{
				LOGGER_EVENT.log(Level.INFO,"Arrived R_MOD is present in MC tbl, Arrived R_MOD RowKey: "+Bytes.toString(mc_result.getRow())); 
			 */
			LOGGER_EVENT.log(Level.INFO,"Check: Arrived Pack is Present in PackInfo Tbl: "+token[1]);
			/*
			blms_pack_info_tblConn = getTable(blms_pack_info);
			Scan scan = new Scan(); 
			FilterList allFilters_pkinfo = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			allFilters_pkinfo.addFilter(new PrefixFilter(Bytes.toBytes(token[1]))); //<- AP
			scan.setFilter(allFilters_pkinfo);
			scan.setReversed(true); // read the latest available key and values for this AP
			scan.setMaxResultSize(1);
			LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+scan.toString());
			scanner = blms_pack_info_tblConn.getScanner(scan);
			Result result = scanner.next();
			 */
			Result result =  getLatestRecord(blms_pack_info,token[1]);
			if(result != null)
			{
				PACK_INFO_TBL = result.getFamilyMap(cf_pk);
				LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_blms_id: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id)));
				LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_vin: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
				LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_upd: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_upd)));
				LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo pkinfo_orignalKey: "+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
				LOGGER_EVENT.log(Level.INFO,"latest key value from packinfo \"pk:3\": "+Bytes.toString(PACK_INFO_TBL.get(Bytes.toBytes("3"))));
				String cond1_blms_pack_id = Bytes.toString(PACK_INFO_TBL.get(pkinfo_blms_id));
				String cond2_r_mod = token[4]; //<- R_Mod
				//byte [] cond2_r_mod_byte = Bytes.toBytes(token[4]);
				String cond2_pkinfo_vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
				LOGGER_EVENT.log(Level.INFO,"cond1_blms_pack_id: "+cond1_blms_pack_id);
				LOGGER_EVENT.log(Level.INFO,"cond2_r_mod: "+cond2_r_mod);
				if(cond1_blms_pack_id != null && !cond1_blms_pack_id.isEmpty()) {	
					LOGGER_EVENT.log(Level.INFO,"Packinfo Contains value for BLMSPACK_ID.."+cond1_blms_pack_id);
					//blms id present, arrived alliance pack present.: true
					LOGGER_EVENT.log(Level.INFO,"Next check, Arrived r_mod: "+cond2_r_mod+", really attached to this Pack: "+token[1]);
					//Iterate over ModList and check the presence:
					boolean flag = false;
					for(int i=3;i<=97;i=i+2)
					{
						byte [] r_mod_q = Bytes.toBytes(String.valueOf(i));
						byte [] r_mod_dt_q = Bytes.toBytes(String.valueOf(i+1));
						String r_mod = Bytes.toString(PACK_INFO_TBL.get(r_mod_q));
						//if(r_mod!=null && Bytes.toString(cond2_r_mod_byte).compareTo(r_mod)==0) {
						if(r_mod!=null && cond2_r_mod.compareTo(r_mod)==0) {
							LOGGER_EVENT.log(Level.INFO,"Arrived R_MOD is really Attached to Arrived Pack..");
							LOGGER_EVENT.log(Level.INFO,"Detaching R_MOD from Arrived Pack..");
							PACK_INFO_TBL.remove(r_mod_q);
							PACK_INFO_TBL.remove(r_mod_dt_q);
							PACK_INFO_TBL.put(PkInfo_FLD_r, Bytes.toBytes(token[10])); //Set inputEventFilename as pmlu event registering, AMO cols
							PACK_INFO_TBL.put(PkInfo_FLD_q, Bytes.toBytes(V_READ_DATE)); //Set ReadDate for this CSV registration, AMO cols
							flag = true;
							break;
						}
					}
					if (!flag) {
						LOGGER_EVENT.log(Level.INFO,"ERROR CASE: R-MOD-"+token[4]+" is not attached in Pack_info tbl for removing Pack.."+token[1]);
						JOB_DETAILS.put(line_cnt,"ERROR CASE: R-MOD-"+token[4]+" is not attached in Pack_info tbl for removing Pack.."+token[1]);
					}else {
						//Update the Packinfo, UpdIdx, vinIdx tbl for new rowkey-values
						LOGGER_EVENT.log(Level.INFO,"Preparing Index Updates for VIN_IDX_TBL: "+cond2_pkinfo_vin);
/*						blms_pack_info_vin_idx_tblConn = getTable(blms_pack_info_vin_idx);
						Scan vin_scan = new Scan(); 
						FilterList allFilters_vin = new FilterList(FilterList.Operator.MUST_PASS_ALL);
						allFilters_vin.addFilter(new PrefixFilter(PACK_INFO_TBL.get(pkinfo_vin))); //<- vin key
						vin_scan.setFilter(allFilters_vin);
						vin_scan.setReversed(true); // read the latest available key and values for this AP
						vin_scan.setMaxResultSize(1);
						LOGGER_EVENT.log(Level.INFO,"scan object to string before execute: "+vin_scan.toString());
						vin_scanner = blms_pack_info_vin_idx_tblConn.getScanner(vin_scan);
						Result vin_result = vin_scanner.next();
*/
						Result vin_result =  getLatestRecord(blms_pack_info_vin_idx,Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin)));
						if(vin_result != null)
						{   
							VIN_IDX_TBL = vin_result.getFamilyMap(cf_idx);
							String vin = Bytes.toString(PACK_INFO_TBL.get(pkinfo_vin));
							byte [] newVinIdxKey = Bytes.toBytes(vin.concat(row_upd));
							Put NewVinIdx_kv = new Put(newVinIdxKey); 
							VIN_IDX_TBL.put(pkinfo_upd,Bytes.toBytes(row_upd));
							LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_upd)+": "+row_upd);		    	 			
							for (Entry<byte[], byte[]> entry : VIN_IDX_TBL.entrySet()) {
								byte[] key = entry.getKey();
								byte[] value = entry.getValue();
								LOGGER_EVENT.log(Level.INFO,Bytes.toString(key)+":"+Bytes.toString(value));

								//NewVinIdx_kv.addColumn(cf_idx, key, row_now_ts, value);
								NewVinIdx_kv.addColumn(cf_idx, key, value);
							}

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Started for the event Line Number: "+line_cnt);
							putInTable(blms_pack_info_vin_idx,NewVinIdx_kv);

							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl Insert Finished for the event Line Number: "+line_cnt);
							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx Updated for New record insert.."+Bytes.toString(newVinIdxKey));
						}else {//no PVLC or PVLU so far for this Pack
							LOGGER_EVENT.log(Level.INFO,"blms_pack_info_vin_idx tbl doesn't have index for arrived Pack.."+token[1]);
						}
						//Prepare new record for:upd_idx_tbl
						String AP = Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey));
						LOGGER_EVENT.log(Level.INFO,"Preparing Index Updates for:upd_idx_tbl..");
						byte [] newUpdIdxKey = Bytes.toBytes(row_upd.concat(AP));
						Put NewUpdIdx_kv = new Put(newUpdIdxKey);
						//NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd, row_now_ts, Bytes.toBytes(row_upd));
						//NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk, row_now_ts, PACK_INFO_TBL.get(pkinfo_orignalKey));
						NewUpdIdx_kv.addColumn(cf_idx,pkinfo_upd, Bytes.toBytes(row_upd));
						NewUpdIdx_kv.addColumn(cf_idx,pkinfo_alliancePk, PACK_INFO_TBL.get(pkinfo_orignalKey));
						LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_upd)+":"+row_upd);
						LOGGER_EVENT.log(Level.INFO,"Updated Column:"+Bytes.toString(pkinfo_alliancePk)+":"+Bytes.toString(PACK_INFO_TBL.get(pkinfo_orignalKey)));
						LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Started for the event Line Number: "+line_cnt);
						putInTable(blms_pack_info_upd_idx,NewUpdIdx_kv);

						LOGGER_EVENT.log(Level.INFO,"blms_pack_info_upd_idx tbl Insert Finished for the event Line Number: "+line_cnt);
						LOGGER_EVENT.log(Level.INFO,"End UPD_idx Updated for New record insert.."+Bytes.toString(newUpdIdxKey));

						//Preparing new record for packInfo:
						LOGGER_EVENT.log(Level.INFO,"Preparing new record for packInfo..");
						PACK_INFO_TBL.put(pkinfo_upd,Bytes.toBytes(row_upd));

						byte [] newPackInfoKey = Bytes.toBytes(AP.concat(row_upd));
						Put NewPackInfo_kv = new Put(newPackInfoKey); 
						for (Entry<byte[], byte[]> entry : PACK_INFO_TBL.entrySet()) {
							byte[] key = entry.getKey();
							byte[] value = entry.getValue();
							LOGGER_EVENT.log(Level.INFO,Bytes.toString(key)+":"+Bytes.toString(value));
							//NewPackInfo_kv.addColumn(cf_pk,key, row_now_ts, value);
							NewPackInfo_kv.addColumn(cf_pk,key, value);
						}
						LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Started for the event Line Number: "+line_cnt);
						putInTable(blms_pack_info,NewPackInfo_kv);

						LOGGER_EVENT.log(Level.INFO,"blms_pack_info tbl Insert Finished for the event Line Number: "+line_cnt);
						//end of new record for packInfo insert 
						LOGGER_EVENT.log(Level.INFO,"End of new record for packInfo insert.."+Bytes.toString(newPackInfoKey));

						//@@Insert successful event in cash_processed_tbl 
						LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert Started for the event Line Number: "+line_cnt);
						putIntoCashProcessed(token);
						LOGGER_EVENT.log(Level.INFO,"CashProcessed tbl Insert finished for the event Line Number: "+line_cnt);

						JOB_DETAILS.put(line_cnt,"success");
						LOGGER_EVENT.log(Level.INFO,"Set:- JOB_DETAILS:"+String.valueOf(line_cnt)+"=success");

					}//remove applied happened on packinfo
				} else { //blmsPack check in packinfo
					LOGGER_EVENT.log(Level.INFO,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
					JOB_DETAILS.put(line_cnt,"ERROR CASE: BLMS PACK ID is not present in Pack_info tbl for removing Pack.."+token[1]);
				}
			}else { //packinfo result check
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived Pack is not Present in PackInfo Table.."+token[1]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived Pack is not Present in PackInfo Table.."+token[1]);
			}
			/*}else {//mc tbl check
				LOGGER_EVENT.log(Level.INFO,"ERROR CASE: Arrived R_MOD is not present in MC tbl, Arrived R_MOD: "+token[4]);
				JOB_DETAILS.put(line_cnt,"ERROR CASE: Arrived R_MOD is not present in MC tbl, Arrived R_MOD: "+token[4]);
			} */
		}catch(Exception e) {
			//LOGGER_EVENT.log(Level.SEVERE,"Error in pmluRemoveApply()..");
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			throw new Exception("SEVERE:Exception in pmluRemoveApply().."+e.getMessage());
		}
	} //end of pmluRemoveApply

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
		String newCPKey = row_upd.concat(token[1]).concat(token[4]);
		Put NewCashProcessed_kv = new Put(Bytes.toBytes(newCPKey));  //token[1] <-AP;token[4] <- vin
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_event_id, Bytes.toBytes(token[0])); //token[0]: event_type.
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_orignalKey,  Bytes.toBytes(token[1]));
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_Timestamp,  Bytes.toBytes(token[2])); //token[2] <- battery Setting Date
		NewCashProcessed_kv.addColumn(cf_cp,pkinfo_upd,  Bytes.toBytes(row_upd));
		NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_001,  Bytes.toBytes(token[3]));
		NewCashProcessed_kv.addColumn(cf_cp,CP_V_COL_002,  Bytes.toBytes(token[4]));


		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_r,  Bytes.toBytes(token[10]));    //Set inputEventFilename as pmlu event registering
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_q,  Bytes.toBytes(V_READ_DATE));  //Set ReadDate for this CSV registration.

		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_s,  Bytes.toBytes(PkInfo_VAL_s)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_t,  Bytes.toBytes(PkInfo_VAL_t)); 
		NewCashProcessed_kv.addColumn(cf_cp,PkInfo_FLD_u,  Bytes.toBytes(row_upd)); 
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
		//fh_pmlu = new FileHandler(PMLU_UPDATES_LOG+"_" + JOB_UPD.replaceAll("[^0-9]", ""));
		fh_pmlu = new FileHandler(PMLU_UPDATES_LOG+"_" + WF_ID.replaceAll("[^0-9]", ""));
		fh_pmlu.setFormatter(formatter);
		LOGGER_EVENT.addHandler(fh_pmlu);
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

			if(conn!=null)
			{
				conn.close();
				LOGGER_EVENT.log(Level.INFO,"conn.close() executed...");
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
			if(conf!=null)
			{
				conf.clear();
				LOGGER_EVENT.log(Level.INFO,"conf.clear() executed...");
			}
			if(fh_pmlu !=null)
			{   LOGGER_EVENT.log(Level.INFO,"Calling fh_pmlu.flush() & fh_pmlu.close()...");
			fh_pmlu.flush();
			fh_pmlu.close();
			}
		}catch(Exception e1) {
			e1.printStackTrace();
			throw new Exception("SEVERE:Exception in releaseResources()...."+e1.getMessage());
		}
	}

	public static void main(String args[]) throws Exception{
		try {
			if(args.length !=3) {
				throw new IOException("SEVERE:PMLU JAVA process Requires[filePath, newUPD, V_READ_DATE], number of passed args not matching with expected..."); 
			}else 
			{
				filePath = new Path(args[0]);
				jdCSVFilePath = new Path(args[0] + "_JOBDETAILS");
				JOB_UPD=args[1];
				V_READ_DATE=args[2]+" +0000";
				//Initialize the LogFile for PVLU,PMLU,MASTER,connection
				getResources(args[2]);
				printInitialValues();
				LOGGER_EVENT.log(Level.INFO,"Log file initiated for the day:- "+JOB_UPD);
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




