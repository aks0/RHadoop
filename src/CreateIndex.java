/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.*;
import java.util.*;

/**
 * 
 * @author sharvanath
 * 
 * 
 */
public class CreateIndex {

	static class FileSplit {
		public String filePath;
		public long start;
		public long length;
		public String[] hosts;
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) throws Exception {
		// TODO code application logic here
		HashMap<String, FileSplit> fsckMap = new HashMap<String, FileSplit>();
		if (args.length < 2) {
			System.err.println("Usage: CreateIndex fsckDump indexFile ");
		}

		String fsckFile = args[0];
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					fsckFile)));

			String line = null;
			String currPath = null;
			String[] hosts, hostSplits;
			long len, offset;
			String hostStr;

			while ((line = reader.readLine()) != null) {
				if (line.startsWith("/") && !line.contains("0 block"))
					currPath = line.split(" ")[0];
				else if (line.startsWith("Status:"))
					break;
				else if (!line.trim().equals("") && currPath != null
						&& line.contains("len=")) {

					len = Integer.parseInt(line.split("len=")[1].split(" ")[0]);
					offset = Integer.parseInt(line.split("offset=")[1]
							.split(" ")[0]);
					hostStr = line.split("\\[")[1].split("\\]")[0];
					hostSplits = hostStr.split(",");
					hosts = new String[hostSplits.length];
					for (int i = 0; i < hostSplits.length; i++)
						hosts[i] = hostSplits[i].split(":")[0].trim();

					FileSplit fs = new FileSplit();
					fs.length = len;
					fs.hosts = hosts;
					fs.filePath = currPath;
					fs.start = offset;
					fsckMap.put(line.split("_")[1], fs);
				}
			}

			String indexFile = args[1];
			reader = new BufferedReader(new FileReader(new File(indexFile)));
			line = reader.readLine();
			while ((line = reader.readLine()) != null) {
				String chunk = line.split(" ")[0].trim();
				String lower = line.split(" ")[1].trim();
				String higher = line.split(" ")[2].trim();
				String outStr = lower + " " + higher + " ";
				FileSplit fs = fsckMap.get(chunk);
				if (fs == null)
					continue;
				outStr += fs.filePath + " ";
				outStr += fs.start + " ";
				outStr += fs.length + " ";
				outStr += Arrays.toString(fs.hosts);
				System.out.println(outStr);
			}

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}
}
