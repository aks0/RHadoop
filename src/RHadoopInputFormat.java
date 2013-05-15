/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//sharva
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An {@link InputFormat} for plain text files. Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line. Keys are
 * the position in the file, and values are the line of text..
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RHadoopInputFormat extends FileInputFormat<LongWritable, Text> {
	/**
	 * Generate the list of files and make them into FileSplits.
	 * 
	 * @param job
	 *            the job context
	 * @throws IOException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		Path path = new Path("/RHadoop/indexLog");
		FileSystem fs = path.getFileSystem(job.getConfiguration());

		HashSet<InputSplit> splits = chunkIndex(fs, path);

		return new ArrayList<InputSplit>(splits);
	}

	// just copied from textinputformat, assuming the key value pairs are
	// separated by the same delimiter
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		String delimiter = context.getConfiguration().get(
				"textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		/* sharvanath : solve this issue */
		// to be implemented
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new LineRecordReader(recordDelimiterBytes);

	}

	public boolean isValidKeyRange(String lower, String upper) {
		return true;
	}

	public boolean isValidKey(String key) {
		return isValidKeyRange(key, key);
	}

	@SuppressWarnings("deprecation")
	public HashSet<InputSplit> chunkIndex(FileSystem fs, Path filenamePath) {

		HashSet<InputSplit> splitSet = new HashSet<InputSplit>();
		try {
			// reading
			FSDataInputStream in = fs.open(filenamePath);
			String line;
			Path file = null;
			long start = 0;
			long length = 0;
			Log LOG = LogFactory.getLog(FileInputFormat.class);

			line = in.readLine();
			LOG.fatal("RHadoop : opened index file, first line = " + line);
			while (line != null) {
				String[] splits = line.split(" ");
				if (splits.length < 5) throw new Exception ("Less Than 5 splits");
				
				String lower = splits[0], upper = splits[1];
				file = new Path(splits[2]);
				start = Long.parseLong(splits[3]);
				length = Long.parseLong(splits[4]);

				splits = line.split("\\[");
				if (splits.length != 2) throw new Exception ("[ split not 2.");
				
				String hostStr = splits[1];
				hostStr = hostStr.substring(0, hostStr.length() - 1);
				String[] hostSplits = hostStr.split(",");
				for (int i = 0; i < hostSplits.length; i++)
					hostSplits[i] = hostSplits[i].trim();

				if (isValidKeyRange(lower, upper))
					splitSet.add(new FileSplit(file, start, length, hostSplits));
				line = in.readLine();
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return splitSet;
	}

}