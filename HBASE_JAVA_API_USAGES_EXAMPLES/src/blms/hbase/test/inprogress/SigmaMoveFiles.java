package blms.hbase.test.inprogress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SigmaMoveFiles {

	private static Path SourceDirectoryPath;
	private static Path DestinationFilePath;
	//private static ArrayList<Path> FaildFiles = new ArrayList<Path>();
	private  static FSDataOutputStream outPutStream;
	private static Logger LOGGER_EVENT;
	private  static BufferedReader br = null;
	private  static BufferedReader br_actual_file = null;
	private static FileHandler fh_log;
	///private static final String PropertyFileName = "/home/asp20571bdp/DEV/BDP_BATCHES/COMMON_UTILS/PROPERTIES/conf_blmb0102.properties";
	private static String wf_id;//{TWS-jobId}
	private static String SigmaMoveFilesType;//{ECU/Diag}
	static int line_cnt = 0; 
	static int file_line_cnt = 0;

	public final static void getResources() throws SecurityException, IOException {
		LOGGER_EVENT = Logger.getLogger(SigmaMoveFiles.class.getName());
		SimpleFormatter formatter = new SimpleFormatter();
		//Handler for PMLU 	
		fh_log = new FileHandler(SigmaMoveFilesType+"_" + wf_id);
		fh_log.setFormatter(formatter);
		LOGGER_EVENT.addHandler(fh_log);
		LOGGER_EVENT.setUseParentHandlers(false);
	}

	private static void WriteToCsvErrFile(String line, String Filename) throws Exception{
		String completeLine = line + "\t" + Filename;
		try {
			outPutStream.writeUTF(completeLine);
			outPutStream.writeUTF("\n");
			outPutStream.flush();
			//LOGGER_EVENT.log(Level.INFO,"Successfuly inserted into CSV, for Event line number: "+line_cnt);
		} catch (IOException e) {
			//LOGGER_EVENT.log(Level.SEVERE,"Error In WriteToCsvErrFile(), failed to Write into hdfs filesystem.."+jdCSVFilePath);
			LOGGER_EVENT.log(Level.SEVERE,e.toString(),e);
			//releaseResources();
			///throw new Exception("Exception in WriteToCsvErrFile()....."+e.getMessage());
			long end_time = System.currentTimeMillis();
			LOGGER_EVENT.info("end_time: "+end_time); 	
			releaseResources();
			System.exit(1);

		}
	}


	private static void releaseResources() throws Exception{
		try {

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
			if(br_actual_file!=null)
			{
				br_actual_file.close();
				LOGGER_EVENT.log(Level.INFO,"br_actual_file.close() executed...");
			}
			if(fh_log !=null)
			{
				LOGGER_EVENT.log(Level.INFO," Executing fh_pvlu.flush() & fh_pvlu.close()...");
				fh_log.flush();
				fh_log.close();
			}
		}catch(IOException e1) {
			e1.printStackTrace();
			//throw new Exception("Exception in releaseResources()...."+e1.getMessage());
			
			System.exit(1);
		}
	}

	static public void main(String args[]) throws Exception{
		try {

			if(args.length !=4) 
			{
				throw new IOException("Pls Provide <SourceDirectoryPath> <DestinationFilePath> <SigmaMoveFilesType> <wf_id>"); 
			}else 
			{
				SourceDirectoryPath = new Path(args[0]);
				DestinationFilePath = new Path(args[1]);
				SigmaMoveFilesType = args[2];
				wf_id = args[3];

				getResources();
				
				long st_time = System.currentTimeMillis();
				LOGGER_EVENT.info("st_time: "+st_time);

				Configuration conf = new Configuration(); 

				FileSystem srcFs = SourceDirectoryPath.getFileSystem(conf); 
				FileSystem dstFs = DestinationFilePath.getFileSystem(conf); 


				if(!srcFs.exists(SourceDirectoryPath)) 
					throw new IOException("<SourceDirectoryPath> does'nt exist: "+SourceDirectoryPath);
				//FaildFiles.add(SourceDirectoryPath);
				/*
		FileSystem dstFs = DestinationFilePath.getFileSystem(conf); 
		if(!dstFs.exists(DestinationFilePath)) 
			dstFs.mkdirs(DestinationFilePath);
		Boolean status = FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);
				 */

				//dstFs = FileSystem.get(conf);
				outPutStream = dstFs.create(DestinationFilePath, true);

				//outPutStream = dstFs.append(DestinationFilePath);

				FileStatus[] status = srcFs.listStatus(SourceDirectoryPath);
				String line;

				for (int i = 0; i < status.length; i++) {

					file_line_cnt=0;
					//line_cnt = 0; //for each file count
					//String SubFileName = status[i].getPath().getName();
					//LOGGER_EVENT.log(Level.INFO,"subfilepath:"+SubFileName);

					Path FileNamePath =  status[i].getPath();
					///LOGGER_EVENT.log(Level.INFO,"subfilepath:"+FileNamePath);

					br = new BufferedReader(new InputStreamReader(srcFs.open(status[i].getPath())));

					//LOGGER_EVENT.log(Level.INFO,"In BufferReader..subfilepath:"+status[i].getPath().toString());
					while ((line = br.readLine()) != null) 
					{	

						String token[] =  line.split("\t");
						//Path SubFileNamePath = (Path) Paths.get(token[1]);
						Path SubFileNamePath = new Path(token[1]);
						///LOGGER_EVENT.log(Level.INFO,"ActulFileName recieved from List: "+SubFileNamePath);
						try {


							if(!srcFs.exists(SubFileNamePath))
							{
								LOGGER_EVENT.log(Level.INFO,"<SourceDirectoryPath> does'nt exist: "+ SubFileNamePath);
								///throw new IOException("<SourceDirectoryPath> does'nt exist: "+SubFileNamePath);
								continue;
							}

							//FileSystem srcFs_actualFile = SubFileNamePath.getFileSystem(conf); 
							//FileStatus[] status_actualFile = srcFs_actualFile.listStatus(SubFileNamePath);
							FileStatus[] status_actualFile = srcFs.listStatus(SubFileNamePath);

							//for (int j = 0; j < status.length; j++) {

							line_cnt = 0;

							//br_actual_file =new BufferedReader(new InputStreamReader(srcFs.open(status_actualFile[j].getPath())));
							br_actual_file =new BufferedReader(new InputStreamReader(srcFs.open(SubFileNamePath)));

							String SubFileName = status_actualFile[0].getPath().getName();
							///LOGGER_EVENT.log(Level.INFO,"ActulFileName recieved using status: "+SubFileName);

							String line_actualfile;
							while ((line_actualfile = br_actual_file.readLine()) != null) 
							{	
								line_cnt++;
								//LOGGER_EVENT.log(Level.INFO,"Token lenght:-"+token.length);
								//LOGGER_EVENT.log(Level.INFO,"Input lineCnt taken for processing: "+line_cnt);
								//LOGGER_EVENT.log(Level.INFO,"Input Event Record for Processing: "+token[0]+" "+token[1]+" "+token[2]+" "+token[3]+" "+token[4]);
								WriteToCsvErrFile(line_actualfile, SubFileName );
							}

							///LOGGER_EVENT.log(Level.INFO,"ActulFileName inserted in csv: "+SubFileName + " & LineCount: "+line_cnt);
							br_actual_file.close();
							//}

							file_line_cnt++;

						}catch(Exception e) {
							LOGGER_EVENT.log(Level.INFO,"Exception: "+ e.getMessage());
						}

					}
					///LOGGER_EVENT.log(Level.INFO,"NewFileList Path : "+FileNamePath + " & LineCount: "+file_line_cnt);	
					br.close();
				}

				outPutStream.close();
				long end_time = System.currentTimeMillis();
				LOGGER_EVENT.info("end_time: "+end_time); 			
			}


		}catch( Exception e) {
			LOGGER_EVENT.log(Level.INFO,"Exception:"+ e.getMessage());
			long end_time = System.currentTimeMillis();
			LOGGER_EVENT.info("end_time: "+end_time); 	
			releaseResources();
			System.exit(1);
			//throw new Exception("Exception:"+ e.getMessage());
		}
		releaseResources();
		System.exit(0);
	}
}
