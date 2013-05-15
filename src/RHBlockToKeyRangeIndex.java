/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

public class RHBlockToKeyRangeIndex {

	public static class RMapper extends Mapper<Object, Text, Text, Text> {

		Log LOG = LogFactory.getLog(Mapper.class);
		boolean isFirst = true;
		String firstKey = null;
		String lastKey = null;
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strValue = value.toString().trim();
			int keyIndex = strValue.indexOf("\t");
			if (keyIndex == -1)
				return;
			
			if (isFirst) {
				firstKey = strValue.substring(0, keyIndex);
				isFirst = false;
			}
			lastKey = strValue.substring(0, keyIndex);
		}

		public void run(Context context) throws IOException,
				InterruptedException {
			setup(context);

			boolean hasNext = true;
			long startTime2;
			long nextKeyValueTime = 0;
			long mapTime = 0;

		    long startTime = System.nanoTime();
		    hasNext = context.nextKeyValue();
		    nextKeyValueTime += System.nanoTime() - startTime;

		    while (hasNext) {
		      startTime2 = System.nanoTime();
		      map(context.getCurrentKey(), context.getCurrentValue(), context);
		      mapTime += System.nanoTime() - startTime2;
		      
		      startTime2 = System.nanoTime();
		      hasNext = context.nextKeyValue();
		      nextKeyValueTime += System.nanoTime() - startTime2;
		    }
		    long timeTaken = System.nanoTime() - startTime;
			
			LOG.fatal("^ MapperJob: " + context.getTaskAttemptID()
					+ ", StartKey: " + firstKey
					+ ", EndKey: " + lastKey);
		    
			LOG.fatal("^ MapperJob: " + context.getTaskAttemptID()
					+ ", RunTimeTaken1(ns): " + timeTaken);

			LOG.fatal("^ MapperJob: " + context.getTaskAttemptID()
					+ ", NextKeyValueTime(ns): " + nextKeyValueTime);

			LOG.fatal("^ MapperJob: " + context.getTaskAttemptID()
					+ ", MapTime(ns): " + mapTime);

			cleanup(context);
		}
	}

	public static class RReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: rhblockindex <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "rhblockindex");
		job.setJarByClass(RHBlockToKeyRangeIndex.class);
		job.setMapperClass(RMapper.class);
		job.setCombinerClass(RReducer.class);
		job.setReducerClass(RReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
