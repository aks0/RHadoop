
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


public class ProcessLogs_ReducerKeys {

	static String jobID;
	static int numDataNodes;
	static DataNode[] allDataNodes;
	static int numMappers;
	static int numReducers;
	static Mapper[] allMappers;
	static Reducer[] allReducers;
	static String outputFileName = "";
	static HashMap <String, String> ipToHostName;
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
		allDataNodes = new DataNode[numDataNodes];
		numMappers = Integer.parseInt(args[2]);
		numReducers = Integer.parseInt(args[3]);
		outputFileName = args[4];
		
		allMappers = new Mapper[numMappers + 1];
		allReducers = new Reducer[numReducers + 1];
		ipToHostName = new HashMap <String, String>();
		Arrays.fill(allMappers, null);
		Arrays.fill(allReducers, null);
		
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
			if (!syslog.exists()) continue;
			
			int mapperid = getMapperIDFromMapperLog (syslog);
			if (mapperid == -1) continue;
			
			String value =
					processMapperEntry(getMapperEntry(syslog, "RunTimeTaken1"));
			allMappers[mapperid].runTime = Long.parseLong(value);

//			value = processMapperEntry(getMapperEntry(syslog, "IPAddress"));
//			allMappers[mapperid].ipaddress = value;
			
			value = processMapperEntry(getMapperEntry(syslog, "NextKeyValueTime"));
			allMappers[mapperid].nextKeyTime1 = Long.parseLong(value);
			
			value = processMapperEntry(getMapperEntry(syslog, "MapTime"));
			allMappers[mapperid].mapTime1 = Long.parseLong(value);
			
			value = processMapperEntry(getMapperEntry(syslog, "ReadTimeTaken"));
			allMappers[mapperid].nextKeyTime2 = Long.parseLong(value);
			
			value = processMapperEntry(getMapperEntry(syslog, "RunTimeTaken2"));
			allMappers[mapperid].mapTime2 = Long.parseLong(value);
		}
		
		/** Read Reducer Logs and find the keys */
 		logLocationName = String.format(
			"/memex/akshay/project/hadoop-runtime/logs/userlogs/application_%s",
			jobID);
		logLocation = new File(logLocationName);
		if (!logLocation.isDirectory())
			throw new IllegalArgumentException ("Wrong Apps Log location");
		for (File container : logLocation.listFiles()) {
			File syslog = new File(container.getAbsolutePath() + "/syslog");
			if (!syslog.exists()) continue;
			processReducerLog (syslog);
		}

		writeStats();
		writeStatsCSV();
		writeStatsReducersIndex();
	}

	private static void processReducerLog (File syslog)
			throws FileNotFoundException {
		
		Scanner logReader = new Scanner(syslog);
		String reducerEntry = null;
		while (logReader.hasNextLine()) {
			reducerEntry = logReader.nextLine();
			if (reducerEntry.contains("FATAL") &&
				reducerEntry.contains("ReducerJob"))
				break;
			reducerEntry = null;
		}
		if (reducerEntry == null) {
			logReader.close();
			return;
		}
		
		String parts[] = null;
		// Extract Reducer ID
		parts = reducerEntry.split("\\^")[1].split(":");
		parts = parts[1].split("_");

		if (parts.length != 6 ||
			!parts[0].trim().equals("attempt") ||
			!parts[3].trim().equals("r")) {
			logReader.close();
			return;
		}
		int reducerid = Integer.parseInt(parts[4].trim());
		if (allReducers[reducerid] == null)
			System.err.println ("Wrong reducer " + reducerid);
		
		// Extract the keys
		while (logReader.hasNextLine()) {
			reducerEntry = logReader.nextLine();
			if (!reducerEntry.contains("Key"))
				continue;
			parts = reducerEntry.split("\\^")[1].split(",")[1].split(":");
			allReducers[reducerid].keys.add(parts[1].trim() + ":"
						   				  + parts[2].trim());
		}
		logReader.close();
	}
	
	private static int getMapperIDFromMapperLog (File syslog)
							throws FileNotFoundException {
		String mapperEntry = getMapperEntry(syslog, "RunTimeTaken1");
		if (mapperEntry == null) return -1;
		
		String[] parts = mapperEntry.split("\\^")[1].split(",");
		parts = parts[0].split(":")[1].split("_");
		
		if (parts.length != 6 ||
			!parts[0].trim().equals("attempt") ||
			!parts[3].trim().equals("m"))
			return -1;

		int mapperid = Integer.parseInt(parts[4].trim());
		return mapperid;
	}
	
	private static String processMapperEntry(String mapperEntry) {
		String[] parts = mapperEntry.split("\\^")[1].split(",");
		String value = parts[1].split(":")[1].trim();
		return value;
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
		String datanodeIdentifier = 
				"org.apache.hadoop.hdfs.server.datanode.DataNode";
		while (dnLogReader.hasNextLine()) {
			str = dnLogReader.nextLine();
			if (!str.contains("FATAL")) continue;

			// MAPPER task
			if (str.contains(clientTraceIdentifier)) {
				ClientTrace ct = new ClientTrace();
				if (str.contains(jobID + "_m_")) {
					fillClientTrace(ct, str);
					if (!ct.op.equals("HDFS_READ")) continue;
				
					int mapperid = getMapperID (ct.clientID);
					if (mapperid == -1) continue;
					Mapper mapper = null;
					if (allMappers[mapperid] != null)
						mapper = allMappers[mapperid];
					else {
						mapper = new Mapper(mapperid);
						allMappers[mapperid] = mapper;
					}
					
					mapper.incrBytesRead(ct);
					mapper.incrTimeRead(ct);
					datanode.mappers.put(mapperid, mapper);
				}
				// NON_MAPREDUCE task
				else {
					datanode.incrBytesRead(ct);
					datanode.incrTimeRead(ct);
				}
			}
			// REDUCER task
			else if (str.contains(datanodeIdentifier) &&
					 str.contains(jobID + "_r_")) {
				int reducerid = getReducerID (str);
				if (reducerid == -1) continue;
				
				String blockid = getReducerBlockID (str);
				if (blockid == null) continue;

				if (allReducers[reducerid] == null) {
					allReducers[reducerid] = new Reducer(reducerid);
					allReducers[reducerid].ipaddress = datanode.ipaddress;
					datanode.reducers.put(reducerid, allReducers[reducerid]);
				}
				allReducers[reducerid].blockids.add(blockid);
			}
		}
		datanode.numMappers = datanode.mappers.size();
		datanode.numReducers = datanode.reducers.size();
	}

	private static String getReducerBlockID(String str) {
		String token = str.split("\\^")[1];
		if (!token.contains("attempt")) return null;
		
		String[] parts = token.split(",")[2].split(":");
		return parts[1].trim();
	}

	private static int getReducerID(String str) {
		String token = str.split("\\^")[1];
		
		String[] parts;
		if (!token.contains("attempt")) return -1;
		
		parts = token.split(",");
		if (parts.length != 3) return -1;
		
		parts = parts[1].split(":");
		if (parts.length != 2) return -1;
		
		parts = parts[1].split("_");
		if (parts.length != 9 ||
			!parts[1].equals("attempt") ||
			!parts[4].equals("r"))
			return -1;
		return Integer.parseInt(parts[5].trim());
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
		ct.blockID = parts[1].trim();

		// transferTime (ns)
		parts = data.nextToken().split(":");
		ct.transferTime = Long.parseLong(parts[1].trim());

		// waitTime (ns)
		parts = data.nextToken().split(":");
		ct.waitTime = Long.parseLong(parts[1].trim());
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
		
		long localTransferTime;
		long localWaitTime;
		long remoteTransferTime;
		long remoteWaitTime;
		
		Counters () {
			localBytesRead = 0;
			remoteBytesRead = 0;
			
			localTransferTime = 0;
			localWaitTime = 0;
			remoteTransferTime = 0;
			remoteWaitTime = 0;
		}

		void incrBytesRead (ClientTrace ct) {
			if (ct.isLocal) localBytesRead += ct.bytes;
			else remoteBytesRead += ct.bytes;
		}

		void incrTimeRead (ClientTrace ct) {
			if (ct.isLocal) {
				localTransferTime += ct.transferTime;
				localWaitTime += ct.waitTime;
			}
			else {
				remoteTransferTime += ct.transferTime;
				remoteWaitTime += ct.waitTime;
			}
		}
	}

	static class DataNode extends Counters {
		String hostname;
		String ipaddress;
		int numMappers;
		HashMap<Integer, Mapper> mappers;
		int numReducers;
		HashMap<Integer, Reducer> reducers;
		
		DataNode () {
			super();
			hostname = ipaddress = null;
			numMappers = 0;
			mappers = new HashMap<Integer, Mapper>();
			reducers = new HashMap<Integer, Reducer>();
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

	static class Mapper extends Counters{
		int id;
		long runTime;
//		String ipaddress;
		long nextKeyTime1;
		long nextKeyTime2;
		long mapTime1;
		long mapTime2;
		
		Mapper (int id) {
			super();
			this.id = id;
//			this.ipaddress = null;
			this.runTime = -1;
			this.nextKeyTime1 = -1;
			this.nextKeyTime2 = -1;
			this.mapTime1 = -1;
			this.mapTime2 = -1;
		}
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

	private static void writeStats() throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName)));
		
		for (DataNode dn : allDataNodes) {
			pr.println ("DataNode: " + dn.hostname);
			pr.println ("IP-Address: " + dn.ipaddress);
			pr.println ("Num-Mappers: " + dn.numMappers);
			pr.println ("LocalBytes: " + dn.localBytesRead);
			pr.println ("RemoteBytes: " + dn.remoteBytesRead);
			pr.println ("LocalTransferTime"+timeUnit+": " + dn.localTransferTime/TIME_MOD);
			pr.println ("LocalWaitTime"+timeUnit+": " + dn.localWaitTime/TIME_MOD);
			pr.println ("RemoteTransferTime"+timeUnit+": " + dn.remoteTransferTime/TIME_MOD);
			pr.println ("RemoteWaitTime"+timeUnit+": " + dn.remoteWaitTime/TIME_MOD);
			pr.println ();
			Counters counter = new Counters();
			Set<Integer> keys = dn.mappers.keySet();
			TreeSet<Integer> keysSorted = new TreeSet<Integer>(keys);
			
			for (Integer mapperid : keysSorted) {
				pr.println ("Mapper-ID: " + mapperid);
				Mapper mapper = dn.mappers.get(mapperid);
				pr.println ("Run-Time"+timeUnit+": " + mapper.runTime/TIME_MOD);
				pr.println ("LocalBytes: " + mapper.localBytesRead);
				pr.println ("RemoteBytes: " + mapper.remoteBytesRead);
				pr.println ("LocalTransferTime"+timeUnit+": " + mapper.localTransferTime/TIME_MOD);
				pr.println ("LocalWaitTime"+timeUnit+": " + mapper.localWaitTime/TIME_MOD);
				pr.println ("RemoteTransferTime"+timeUnit+": " + mapper.remoteTransferTime/TIME_MOD);
				pr.println ("RemoteWaitTime"+timeUnit+": " + mapper.remoteWaitTime/TIME_MOD);
				pr.println ("NextKeyValueTime1"+timeUnit+": " + mapper.nextKeyTime1/TIME_MOD);
				pr.println ("MapTime1"+timeUnit+": " + mapper.mapTime1/TIME_MOD);
				pr.println ("NextKeyValueTime2"+timeUnit+": " + mapper.nextKeyTime2/TIME_MOD);
				pr.println ("MapTime2"+timeUnit+": " + mapper.mapTime2/TIME_MOD);
				
				counter.localBytesRead += mapper.localBytesRead;
				counter.localTransferTime += mapper.localTransferTime;
				counter.localWaitTime += mapper.localWaitTime;
				counter.remoteBytesRead += mapper.remoteBytesRead;
				counter.remoteTransferTime += mapper.remoteTransferTime;
				counter.remoteWaitTime += mapper.remoteWaitTime;
				pr.println ();
			}
			pr.println ();
			pr.printf ("Total Stats for Mappers (%d)\n", dn.numMappers);
			pr.println ("LocalBytes: " + counter.localBytesRead);
			pr.println ("RemoteBytes: " + counter.remoteBytesRead);
			pr.println ("LocalTransferTime"+timeUnit+": " + counter.localTransferTime/TIME_MOD);
			pr.println ("LocalWaitTime"+timeUnit+": " + counter.localWaitTime/TIME_MOD);
			pr.println ("RemoteTransferTime"+timeUnit+": " + counter.remoteTransferTime/TIME_MOD);
			pr.println ("RemoteWaitTime"+timeUnit+": " + counter.remoteWaitTime/TIME_MOD);
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
		pr.println ("MapperID" +
//				 	"\tMapper-IPAddress" +
//					"\tDataNode" +
					"\tRunTime"+timeUnit + 
					"\tLocalBytes" +
					"\tRemoteBytes" +
					"\tLocalTransferTime"+timeUnit +
					"\tLocalWaitTime"+timeUnit +
					"\tRemoteTransferTime"+timeUnit +
					"\tRemoteWaitTime"+timeUnit +
					"\tNextKeyValueTime1"+timeUnit + 
					"\tMapTime1"+timeUnit +
					"\tNextKeyValueTime2"+timeUnit +
					"\tMapTime2"+timeUnit
					);
		String MAPPER_FORMAT = "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d" + 
							   "\t%d\t%d\t%d\t%d\n";
		
		for (Mapper mapper : allMappers) {
			if (mapper == null) continue;
			pr.printf (MAPPER_FORMAT,
					mapper.id,
//					mapper.ipaddress,
//					mapper.ipaddress != null ?
//							ipToHostName.get(mapper.ipaddress) :
//							"null IPAddress",
					mapper.runTime/TIME_MOD,
					mapper.localBytesRead, mapper.remoteBytesRead,
					mapper.localTransferTime/TIME_MOD,
					mapper.localWaitTime/TIME_MOD,
					mapper.remoteTransferTime/TIME_MOD,
					mapper.remoteWaitTime/TIME_MOD,
					mapper.nextKeyTime1/TIME_MOD,
					mapper.mapTime1/TIME_MOD,
					mapper.nextKeyTime2/TIME_MOD,
					mapper.mapTime2/TIME_MOD
					);
		}
		pr.close();
	}
	
	private static void writeStatsReducersIndex() throws IOException {
		PrintWriter pr = new PrintWriter(
						new BufferedWriter(
						new FileWriter(outputFileName + "_reducer")));
		pr.println ("Key" +
					"\tBlockIDs"
					);
		String REDUCER_FORMAT = "%s %s\n";
		
		for (Reducer reducer : allReducers) {
			if (reducer == null) continue;
			for (String key : reducer.keys) {
				pr.printf (REDUCER_FORMAT,
							key,
							reducer.blockids.toString()
						  );
			}
		}
		pr.close();
	}

}
