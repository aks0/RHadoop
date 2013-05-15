
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class ProcessOnTheFlyReducer {

	static String jobID;
	static int numDataNodes;
	static int numReducers;
	static String outputFileName = "";
	static long TIME_MOD = 1000 * 1000;
	static String timeUnit = "(ms)";
	
	public static void main(String[] args) throws IOException {
		if (args.length != 5) {
			System.err.println("Usage: java ProcessLogs " +
					"<Job-Id> <num-data-nodes> <num-mappers>" +
					" <num-reducers> <file-name>");
			System.exit(0);
		}
		
		/** Extract the job-id from the argument
		 * example: job_1366748602842_0001
		 * output: 1366748602842_0001
		 */
		jobID = args[0].substring(4);
		numDataNodes = Integer.parseInt(args[1]);
		numReducers = Integer.parseInt(args[3]);
		outputFileName = args[4];
		
		initLogFiles ();
		
		/** Read Mapper Logs and find the run-times */
 		String logLocationName = String.format(
			"/memex/akshay/project/hadoop-runtime/logs/userlogs/application_%s",
			jobID);
		File logLocation = new File(logLocationName);
		if (!logLocation.isDirectory())
			throw new IllegalArgumentException ("Wrong Apps Log location");
		
		int numReducersDone = 0;
		for (File container : logLocation.listFiles()) {
			File syslog = new File(container.getAbsolutePath() + "/syslog");
			if (!syslog.exists()) continue;
			
			int reducerid = -1;
			Scanner logReader = new Scanner(syslog);
			String read = null;
			String key = null;
			Reducer reducer = null;
			while (logReader.hasNext()) {
				read = logReader.nextLine();
				if (!read.contains("ReducerJob"))	continue;
				if (!read.contains (jobID + "_r_")) continue;
				if (!read.contains("KeyWritten"))	continue;

				if (reducerid == -1) {
					reducerid = getReducerID (read);
					reducer = new Reducer (reducerid);
				}
				
				key = getKeyFromReducerEntry (read);
				reducer.keys.add(key);
			}
			logReader.close();
			if (reducer == null | reducerid == -1) continue;
			
			/** Read from each datanode log and JOIN with the mapper */
			Scanner slaveReader =
				new Scanner(
				new File("/memex/akshay/project/hadoop-runtime/etc/hadoop/slaves"));
			String DN_LOG_FORMAT_NAME = "hadoop-akshay-datanode-%s.local.log";
			for (int i = 0; i < numDataNodes; i++) {
				String dnName = slaveReader.nextLine();
				String logFileName = String.format(DN_LOG_FORMAT_NAME, dnName);
				Scanner dnLogReader =
					new Scanner (
					new File("/memex/akshay/project/hadoop-runtime/logs/"
							+ logFileName));
				processDNClientTrace(reducer, dnLogReader);
				dnLogReader.close();
			}
			slaveReader.close();
			writeStatsReducerIndex(reducer);
			System.out.println (String.format("[%d/%d] ReducerID: %d",
								numReducersDone, numReducers, reducerid));
			numReducersDone++;
		}
	}

	private static String getKeyFromReducerEntry(String read) {
		String token = read.split("\\^")[1];
		if (!token.contains("attempt")) return null;
		
		String[] parts = token.split(",")[1].trim().split(" ");
		if (parts.length != 2) return null;
		
		return parts[1].trim();
	}

	private static void initLogFiles() throws IOException {
		PrintWriter pr = new PrintWriter(
				new BufferedWriter(
				new FileWriter(outputFileName + "_reducer")));
		pr.println ("Key" +
					"\tBlockIDs"
				   );
		pr.close();

	}

	private static void processDNClientTrace(Reducer reducer,
											 Scanner dnLogReader) {
		String str;
		String datanodeIdentifier = 
				"org.apache.hadoop.hdfs.server.datanode.DataNode";
		while (dnLogReader.hasNextLine()) {
			str = dnLogReader.nextLine();
			if (!str.contains("FATAL")) continue;

			// REDUCER task
			if (str.contains(datanodeIdentifier) &&
				str.contains(jobID + "_r_") &&
				str.contains("HDFS_WRITE")) {
			
				int reducerid = getReducerIDDataLog (str);
				if (reducerid == -1) continue;
				if (reducerid != reducer.id) continue;
				
				String blockid = getReducerBlockID (str);
				if (blockid == null) continue;

				reducer.blockids.add(blockid);
			}
		}
	}

	private static String getReducerBlockID(String str) {
		String token = str.split("\\^")[1];
		if (!token.contains("attempt")) return null;
		
		String[] parts = token.split(",")[2].split(":");
		return parts[1].trim();
	}

	private static int getReducerID(String str) {
		String token = str.split("\\^")[1];
		
		String[] parts = token.split(",")[0].split(":");
		
		parts = parts[1].trim().split("_");
		if (parts.length != 6 ||
			!parts[0].equals("attempt") ||
			!parts[3].equals("r"))
			return -1;
		
		return Integer.parseInt(parts[4].trim());
	}

	private static int getReducerIDDataLog(String str) {
		String token = str.split("\\^")[1];
		if (!token.contains("attempt")) return -1;
		
		String[] parts = token.split(",")[1].split(":");
		if (parts.length != 2) return -1;
		
		parts = parts[1].split("_");
		if (parts.length != 9 ||
			!parts[1].equals("attempt") ||
			!parts[4].equals("r"))
			return -1;
		
		return Integer.parseInt(parts[5].trim());
	}

	static class Reducer {
		int id;
		HashSet<String> blockids;
		ArrayList<String> keys;
		String ipaddress;
		
		Reducer (int id) {
			this.id = id;
			this.blockids = new HashSet<String>();
			this.keys = new ArrayList<String>();
			this.ipaddress = null;
		}
	}
	
	private static void writeStatsReducerIndex(Reducer reducer)
						throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName + "_reducer", true)));
		String REDUCER_FORMAT = "%s %s\n";
		
		for (String key : reducer.keys) {
			pr.printf (REDUCER_FORMAT,
						key,
						reducer.blockids.toString()
					);
		}
		pr.close();
	}
}
