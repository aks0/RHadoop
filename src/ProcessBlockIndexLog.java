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

public class ProcessBlockIndexLog {

	static String jobID;
	static int numDataNodes;
	static int numMappers;
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
		numMappers = Integer.parseInt(args[2]);
		outputFileName = args[4];
		
		initLogFiles ();
		
		/** Read Mapper Logs and find the run-times */
 		String logLocationName = String.format(
			"/memex/akshay/project/hadoop-runtime/logs/userlogs/application_%s",
			jobID);
		File logLocation = new File(logLocationName);
		if (!logLocation.isDirectory())
			throw new IllegalArgumentException ("Wrong Apps Log location");
		
		int numMappersDone = 0;
		for (File container : logLocation.listFiles()) {
			File syslog = new File(container.getAbsolutePath() + "/syslog");
			if (!syslog.exists()) continue;
			
			int mapperid = -1;
			Scanner logReader = new Scanner(syslog);
			String read = null;
			Mapper mapper = null;
			while (logReader.hasNext()) {
				read = logReader.nextLine();
				if (!read.contains("MapperJob"))	continue;
				if (!read.contains (jobID + "_m_")) continue;
				if (!read.contains("StartKey"))		continue;

				if (mapperid == -1) {
					mapperid = getMapperID (read);
					mapper = new Mapper (mapperid);
				}
								
				setKeyFromMapperEntry (read, mapper);
			}
			logReader.close();
			if (mapper == null | mapperid == -1) continue;
			
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
				processDNClientTrace(mapper, dnLogReader);
				dnLogReader.close();
			}
			slaveReader.close();
			
			writeStatsMapperIndex(mapper);
			System.out.println (String.format("[%d/%d] MapperID: %d",
								numMappersDone, numMappers, mapperid));
			numMappersDone++;
		}
	}

	private static void setKeyFromMapperEntry(String read, Mapper mapper) {
		String token = read.split("\\^")[1];
		mapper.startKey = token.split(",")[1].split(":")[1].trim() + ":"
						+ token.split(",")[1].split(":")[2].trim();
		mapper.endKey = token.split(",")[2].split(":")[1].trim() + ":"
						+ token.split(",")[2].split(":")[2].trim();
	}

	private static void initLogFiles() throws IOException {
		PrintWriter pr = new PrintWriter(
				new BufferedWriter(
				new FileWriter(outputFileName + "_mapper")));
		pr.println ("BlockID" +
					"\tStartKey" +
					"\tEndKey"
				   );
		pr.close();

	}

	private static void processDNClientTrace(Mapper mapper,
											 Scanner dnLogReader) {
		String str;
		String clientTraceIdentifier = 
				"org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace";
		while (dnLogReader.hasNextLine()) {
			str = dnLogReader.nextLine();
			if (!str.contains("FATAL")) continue;

			// REDUCER task
			if (str.contains(clientTraceIdentifier) &&
				str.contains(jobID + "_m_") &&
				str.contains("HDFS_READ")) {
			
				ClientTrace ct = new ClientTrace();
				fillClientTrace(ct, str);
				
				int mapperid = getMapperIDDataLog (ct.clientID);
				if (mapperid == -1) continue;
				if (mapperid != mapper.id) continue;
				
				String blockid = getMapperBlockID (ct.blockID);
				if (blockid == null) continue;

				mapper.blockids.add(blockid);
			}
		}
	}

	static class ClientTrace {
		String src;
		String srcport;
		String dest;
		String destport;
		boolean isLocal;
		long bytes;
		String op;
		String clientID;
		long offset;
		String srvID;
		String blockID;
		long transferTime;
		long waitTime;
	}

	private static void fillClientTrace(ClientTrace ct, String str) {
		String token = str.split("\\^")[1];
		StringTokenizer data = new StringTokenizer(token, ",");
		
		String[] parts;
		
		// src
		parts = data.nextToken().split(":");
		ct.src = parts[1].trim().substring(1);
		ct.srcport = parts[2].trim();
		
		// dest
		parts = data.nextToken().split(":");
		ct.dest = parts[1].trim().substring(1);
		ct.destport = parts[2].trim();
		
		// isLocal
		parts = data.nextToken().split(":");
		ct.isLocal = parts[1].trim().equals("TRUE") ? true : false;
		
		// bytes
		parts = data.nextToken().split(":");
		ct.bytes = Long.parseLong(parts[1].trim());
		
		// op
		parts = data.nextToken().split(":");
		ct.op = parts[1].trim();
		
		// client ID
		parts = data.nextToken().split(":");
		ct.clientID = parts[1].trim();
		
		// offset
		parts = data.nextToken().split(":");
		ct.offset = Long.parseLong(parts[1].trim());

		// srv ID
		parts = data.nextToken().split(":");
		ct.srvID = parts[1].trim();

		// block ID
		parts = data.nextToken().split(":");
		ct.blockID = parts[2].trim();

		// transferTime (ns)
		parts = data.nextToken().split(":");
		ct.transferTime = Long.parseLong(parts[1].trim());

		// waitTime (ns)
		parts = data.nextToken().split(":");
		ct.waitTime = Long.parseLong(parts[1].trim());
	}

	private static String getMapperBlockID(String str) {
		String[] parts = str.split("_");
		return parts[1].trim();
	}

	private static int getMapperID(String str) {
		String token = str.split("\\^")[1];
		
		String[] parts = token.split(",")[0].split(":");
		
		parts = parts[1].trim().split("_");
		if (parts.length != 6 ||
			!parts[0].equals("attempt") ||
			!parts[3].equals("m"))
			return -1;
		
		return Integer.parseInt(parts[4].trim());
	}

	private static int getMapperIDDataLog(String str) {
		String[] parts = str.split("_");
		if (parts.length != 9 ||
			!parts[1].equals("attempt") ||
			!parts[4].equals("m"))
			return -1;
		
		return Integer.parseInt(parts[5].trim());
	}

	private static void writeStatsMapperIndex(Mapper mapper)
						throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName + "_mapper", true)));
		String MAPPER_FORMAT = "%s %s %s\n";
		
		for (String blockid : mapper.blockids) {
			pr.printf (MAPPER_FORMAT,
						blockid,
						mapper.startKey,
						mapper.endKey
					);
		}
		pr.close();
	}

	static class Mapper {
		int id;
		String startKey;
		String endKey;
		HashSet<String> blockids;
		
		Mapper (int id) {
			this.id = id;
			this.blockids = new HashSet<String>();
			this.startKey = null;
			this.endKey = null;
		}
	}

}
