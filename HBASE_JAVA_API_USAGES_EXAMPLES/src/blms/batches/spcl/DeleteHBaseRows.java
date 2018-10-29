
package blms.batches.spcl;
import java.net.URI;
import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteHBaseRows {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("\nINVALID ARGUMENTS: <<hbase_table>> <<keys_to_delete>>");
			return;
		}

		String hbaseTable = (args[0] != null ? args[0].trim() : null), pathToDelete = (args[1] != null ? args[1].trim() : null);

		if(hbaseTable == null || pathToDelete == null) {
			System.out.println("\nINVALID ARGUMENTS: <<hbase_table>> <<keys_to_delete_dir>>");
			return;
		}
		else {
			Configuration conf = new Configuration();

			Configuration hConf = HBaseConfiguration.create(conf);
			HTable hTable = new HTable(hConf, hbaseTable.replaceAll("hbase://", ""));

			FileSystem fs = FileSystem.get(URI.create(pathToDelete), conf);
			Path path = new Path(pathToDelete);
			if(!fs.exists(path)) {
				System.out.println("\nINVALID ARGUMENTS: Path doest not exist");
				return;
			}

			FileStatus[] status = fs.listStatus(path);

			ArrayList<Delete> keysToDelete = new ArrayList<Delete>();
			String str;
			int numKeys = 0;

			for (int i = 0; i < status.length; i++) { 
				BufferedReader bfr = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				while ((str = bfr.readLine()) != null) {
					String[] data = str.split("[\t]", 2);
					if (data[0] == null || data.length == 0 || data[0].isEmpty()) {						
						continue;
					}
					keysToDelete.add(new Delete(Bytes.toBytes(data[0])));
				}
				if(!keysToDelete.isEmpty()) {
					numKeys += keysToDelete.size();
					hTable.delete(keysToDelete);
					System.out.println(keysToDelete.size()+ " rows are deleted from table: " + hbaseTable);
					keysToDelete.clear();
				}
			}
			System.out.println("Total: " + numKeys + " rows are deleted from table: " + hbaseTable);
		}
	}
}

