import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;


public class ProcessLogs_BackUp {

	static String jobID;
	static int numDataNodes;
	static DataNode[] allDataNodes;
	static int numMappers;
	static Mapper[] allMappers;
	static String outputFileName = "";
	static HashMap <String, String> ipToHostName;
	
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.err.println("Usage: java ProcessLogs " +
					"<Job-Id> <num-data-nodes> <num-mappers>" +
					" <file-name>");
			System.exit(0);
		}
		
		/** Extract the job-id from the argument
		 * example: job_1366748602842_0001
		 * output: 1366748602842_0001
		 */
		jobID = args[0].substring(4);
		numDataNodes = Integer.parseInt(args[1]);
		allDataNodes = new DataNode[numDataNodes];
		numMappers = Integer.parseInt(args[2]);
		outputFileName = args[3];
		
		allMappers = new Mapper[numMappers + 1];
		ipToHostName = new HashMap <String, String>();
		Arrays.fill(allMappers, null);
		
		/** Read from each datanode log */
		Scanner slaveReader =
			new Scanner(
			new File("/memex/akshay/project/hadoop-runtime/etc/hadoop/slaves"));
		String DN_LOG_FORMAT_NAME = "hadoop-akshay-datanode-%s.local.log";
		for (int i = 0; i < numDataNodes; i++) {
			allDataNodes[i] = new DataNode();
			String dnName = slaveReader.nextLine();
			String logFileName = String.format(DN_LOG_FORMAT_NAME, dnName);
			Scanner dnLogReader =
				new Scanner (
				new File("/memex/akshay/project/hadoop-runtime/logs/"
						+ logFileName));
			getDataNodeName (allDataNodes[i], dnLogReader);
			processDNClientTrace(allDataNodes[i], dnLogReader);
			dnLogReader.close();
		}
		slaveReader.close();
		
		/** Read Mapper Logs and find the run-times */
 		String logLocationName = String.format(
			"/memex/akshay/project/hadoop-runtime/logs/userlogs/application_%s",
			jobID);
		File logLocation = new File(logLocationName);
		if (!logLocation.isDirectory())
			throw new IllegalArgumentException ("Wrong Apps Log location");
		for (File container : logLocation.listFiles()) {
			File syslog = new File(container.getAbsolutePath() + "/syslog");
			String mapperEntry1 = getMapperEntry(syslog, "RunTimeTaken");
			if (mapperEntry1 == null) continue;

			String mapperEntry2 = getMapperEntry(syslog, "IPAddress");
			String mapperEntry3 = getMapperEntry(syslog, "ReadTimeTaken");
			
			boolean success = processMapperEntry (mapperEntry1,
											mapperEntry2, mapperEntry3);
			if (!success)
				throw new IllegalArgumentException ("Wrong Mapper log");
		}
		writeStats();
		writeStatsCSV();
	}

	private static boolean processMapperEntry(String mapperEntry1,
											  String mapperEntry2,
											  String mapperEntry3) {
		// Get Run-time for the mapper.
		StringTokenizer data = new StringTokenizer(mapperEntry1, "^");
		data.nextToken();
		String[] parts = data.nextToken().split(",");
		long time = Long.parseLong(parts[1].split(":")[1].trim());
		parts = parts[0].split(":")[1].split("_");
		
		if (parts.length != 6 ||
			!parts[0].trim().equals("attempt") ||
			!parts[3].trim().equals("m"))
			return false;

		int mapperid = Integer.parseInt(parts[4].trim());
		allMappers[mapperid].runTime = time;

		// Get IPAddress for the mapper.
		data = new StringTokenizer(mapperEntry2, "^");
		data.nextToken();
		parts = data.nextToken().split(",");
		String ipaddress = parts[1].split(":")[1].trim();
		allMappers[mapperid].ipaddress = ipaddress;
		
		// Get ReadTime for the mapper.
		data = new StringTokenizer(mapperEntry3, "^");
		data.nextToken();
		parts = data.nextToken().split(",");
		String readTime = parts[1].split(":")[1].trim();
		allMappers[mapperid].readTime = Long.parseLong(readTime);
		return true;
	}
	

	private static String getMapperEntry(File syslog, String query)
					throws FileNotFoundException {
		
		Scanner logReader = new Scanner(syslog);
		String mapperEntry = null;
		while (logReader.hasNextLine()) {
			mapperEntry = logReader.nextLine();
			if (mapperEntry.contains("FATAL") &&
				mapperEntry.contains("MapperJob") &&
				mapperEntry.contains(query)) {
				logReader.close();
				return mapperEntry;
			}
		}
		logReader.close();
		return null;
	}

	private static void processDNClientTrace(DataNode datanode,
			Scanner dnLogReader) {
		String str;
		String clientTraceIdentifier = 
				"org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace";
		while (dnLogReader.hasNextLine()) {
			str = dnLogReader.nextLine();
			if (!str.contains("FATAL")) continue;
			if (!str.contains(clientTraceIdentifier)) continue;
			ClientTrace ct = new ClientTrace();
			fillClientTrace(ct, str);
			if (!ct.op.equals("HDFS_READ")) continue;
			
			// MAPREDUCE task
			if (ct.clientID.contains(jobID)) {
				int mapperid = getMapperID (ct.clientID);
				if (mapperid == -1) continue;
				Mapper mapper = null;
				if (allMappers[mapperid] != null)
					mapper = allMappers[mapperid];
				else {
					mapper = new Mapper(mapperid);
					allMappers[mapperid] = mapper;
				}
				
				mapper.incrBytesRead(ct.bytes, ct.isLocal);
				mapper.incrTimeRead(ct);
				datanode.mappers.put(mapperid, mapper);
			}
			// NON_MAPREDUCE task
			else {
				datanode.incrBytesRead(ct.bytes, ct.isLocal);
				datanode.incrTimeRead(ct);
			}
		}
		datanode.numMappers = datanode.mappers.size();
	}

	/**
	 * Example: DFSClient_attempt_1366748602842_0001_m_000009_0_701453471_1
	 * Output: 9
	 * @param clientID
	 * @return
	 */
	private static int getMapperID(String clientID) {
		String[] parts = clientID.split("_");
		if (parts.length != 9 ||
			!parts[1].equals("attempt") ||
			!parts[4].equals("m"))
			return -1;

		return Integer.parseInt(parts[5]);
	}
	

	private static void fillClientTrace(ClientTrace ct, String str) {
		StringTokenizer data = new StringTokenizer(str, "^");
		
		// discard first token
		String tmp = data.nextToken();
		tmp = data.nextToken();
		data = new StringTokenizer(tmp, ",");
		
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
		ct.blockID = parts[1].trim();

		// localReadTime1 (ns)
		parts = data.nextToken().split(":");
		ct.localReadTime1 = Long.parseLong(parts[1].trim());

		// localReadTime2 (ns)
		parts = data.nextToken().split(":");
		ct.localReadTime2 = Long.parseLong(parts[1].trim());

		// remoteReadTime (ns)
		parts = data.nextToken().split(":");
		ct.remoteReadTime = Long.parseLong(parts[1].trim());

		// remoteSendTime (ns)
		parts = data.nextToken().split(":");
		ct.remoteSendTime = Long.parseLong(parts[1].trim());
	}

	private static void getDataNodeName(DataNode dataNode, Scanner dnLogReader){
		String str;
		while (true) {
			str = dnLogReader.nextLine();
			if (str.contains("host = "))
				break;
		}
		// Extract Host-name
		String[] parts = str.split(":")[1].split("=")[1].split("/");
		dataNode.hostname = parts[0].trim();
		dataNode.ipaddress = parts[1].trim();
		ipToHostName.put(dataNode.ipaddress, dataNode.hostname);
	}

	static class Counters {
		long localBytesRead;
		long remoteBytesRead;
		
		long localReadTime1;
		long localReadTime2;
		long remoteReadTime;
		long remoteSendTime;
		
		Counters () {
			localBytesRead = 0;
			remoteBytesRead = 0;
			
			localReadTime1 = 0;
			localReadTime2 = 0;
			remoteReadTime = 0;
			remoteSendTime = 0;
		}

		void incrBytesRead (long bytes, boolean isLocal) {
			if (isLocal) localBytesRead += bytes;
			else remoteBytesRead += bytes;
		}

		void incrTimeRead (ClientTrace ct) {
			localReadTime1 += ct.localReadTime1;
			localReadTime2 += ct.localReadTime2;
			remoteReadTime += ct.remoteReadTime;
			remoteSendTime += ct.remoteSendTime;
		}
	}

	static class DataNode extends Counters {
		String hostname;
		String ipaddress;
		int numMappers;
		HashMap<Integer, Mapper> mappers;
		
		DataNode () {
			super();
			hostname = ipaddress = null;
			numMappers = 0;
			mappers = new HashMap<Integer, Mapper>();
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
		long localReadTime1;
		long localReadTime2;
		long remoteReadTime;
		long remoteSendTime;
	}
	
	
	static class Mapper extends Counters{
		int id;
		long runTime;
		String ipaddress;
		long readTime;
		
		Mapper (int id) {
			super();
			this.id = id;
			this.ipaddress = null;
			this.runTime = -1;
			this.readTime = -1;
		}
	}

	private static void writeStats() throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName)));
		long TIME_MOD = 1000;
		
		for (DataNode dn : allDataNodes) {
			pr.println ("DataNode: " + dn.hostname);
			pr.println ("IP-Address: " + dn.ipaddress);
			pr.println ("Num-Mappers: " + dn.numMappers);
			pr.println ("LocalBytes: " + dn.localBytesRead);
			pr.println ("RemoteBytes: " + dn.remoteBytesRead);
			pr.println ("LocalReadTime1(us): " + dn.localReadTime1/TIME_MOD);
			pr.println ("LocalReadTime2(us): " + dn.localReadTime2/TIME_MOD);
			pr.println ("RemoteReadTime(us): " + dn.remoteReadTime/TIME_MOD);
			pr.println ("RemoteSendTime(us): " + dn.remoteSendTime/TIME_MOD);
			pr.println ();
			Counters counter = new Counters();
			Set<Integer> keys = dn.mappers.keySet();
			TreeSet<Integer> keysSorted = new TreeSet<Integer>(keys);
			
			for (Integer mapperid : keysSorted) {
				pr.println ("Mapper-ID: " + mapperid);
				Mapper mapper = dn.mappers.get(mapperid);
				pr.println ("Run-Time(us): " + mapper.runTime/TIME_MOD);
				pr.println ("LocalBytes: " + mapper.localBytesRead);
				pr.println ("RemoteBytes: " + mapper.remoteBytesRead);
				pr.println ("LocalReadTime1(us): " + mapper.localReadTime1/TIME_MOD);
				pr.println ("LocalReadTime2(us): " + mapper.localReadTime2/TIME_MOD);
				pr.println ("RemoteReadTime(us): " + mapper.remoteReadTime/TIME_MOD);
				pr.println ("RemoteSendTime(us): " + mapper.remoteSendTime/TIME_MOD);
				pr.println ("ReadTime(us): " + mapper.readTime/TIME_MOD);
				
				counter.localBytesRead += mapper.localBytesRead;
				counter.localReadTime1 += mapper.localReadTime1;
				counter.localReadTime2 += mapper.localReadTime2;
				counter.remoteBytesRead += mapper.remoteBytesRead;
				counter.remoteReadTime += mapper.remoteReadTime;
				counter.remoteSendTime += mapper.remoteSendTime;
				pr.println ();
			}
			pr.println ();
			pr.printf ("Total Stats for Mappers (%d)\n", dn.numMappers);
			pr.println ("LocalBytes: " + counter.localBytesRead);
			pr.println ("RemoteBytes: " + counter.remoteBytesRead);
			pr.println ("LocalReadTime1(us): " + counter.localReadTime1/TIME_MOD);
			pr.println ("LocalReadTime2(us): " + counter.localReadTime2/TIME_MOD);
			pr.println ("RemoteReadTime(us): " + counter.remoteReadTime/TIME_MOD);
			pr.println ("RemoteSendTime(us): " + counter.remoteSendTime/TIME_MOD);
			pr.println ();
			pr.println("-------------------------------------------------" +
					   "-------------------------");
			pr.println ();
		}
		pr.close();
	}
	
	private static void writeStatsCSV() throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName + ".xls")));
		long TIME_MOD = 1000;
		pr.println ("MapperID" +
				 	"\tMapper-IPAddress" +
					"\tDataNode" +
					"\tRunTime(us)" + 
					"\tLocalBytes" +
					"\tRemoteBytes" +
					"\tLocalReadTime1(us)" +
					"\tLocalReadTime2(us)" +
					"\tRemoteReadTime(us)" +
					"\tRemoteSendTime(us)" +
					"\tReadTime(us)"
					);
		String MAPPER_FORMAT = "%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n";
		
		for (Mapper mapper : allMappers) {
			if (mapper == null) continue;
			pr.printf (MAPPER_FORMAT,
					mapper.id, mapper.ipaddress,
					mapper.ipaddress != null ?
							ipToHostName.get(mapper.ipaddress) :
							"null IPAddress",
					mapper.runTime/TIME_MOD,
					mapper.localBytesRead, mapper.remoteBytesRead,
					mapper.localReadTime1/TIME_MOD,
					mapper.localReadTime2/TIME_MOD,
					mapper.remoteReadTime/TIME_MOD,
					mapper.remoteSendTime/TIME_MOD,
					mapper.readTime/TIME_MOD
					);
		}
		pr.close();
	}
}
