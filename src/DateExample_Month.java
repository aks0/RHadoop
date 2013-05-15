//package org.apache.hadoop.examples;

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
//package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.MultiFileWordCount.WordOffset;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.RHadoopInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

public class DateExample_Month {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			long startTime = System.nanoTime();
			IsValidKeyFormat isv = new IsValidKeyFormat();
			
			String strValue = value.toString().trim();
			int keyIndex = strValue.indexOf("\t");
			if (keyIndex == -1) return;
			
			String dateKey = strValue.substring(0, keyIndex);
			
			if (isv.isValidKey(dateKey)) {
				// No reduce jobs
				// O(n) Job on value
				String logValue = strValue.substring(keyIndex);
				for (int i = 0; i < logValue.length(); i++);
				//word.set(dateKey);
				//context.write(word, one);
			}
			runTimeMapper += System.nanoTime() - startTime;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class IsValidKeyFormat extends RHadoopInputFormat {
		class Date {
			int year;
			int month;
			int day;
			int hour;
			int minutes;
			String msg;
			
			Date (String str) {
				if (str == null) return;
				String parts[] = str.split("/");
				if (parts.length != 3) return;
				
				year = Integer.parseInt(parts[0]);
				month = Integer.parseInt(parts[1]);
				
				parts = parts[2].split("-");
				if (parts.length != 3) return;
				
				day = Integer.parseInt(parts[0]);
				msg = parts[2];
				
				parts = parts[1].split(":");
				if (parts.length != 2) return;
				
				hour = Integer.parseInt(parts[0]);
				minutes = Integer.parseInt(parts[0]);
			}
		}
		
		@Override
		public boolean isValidKeyRange(String lower, String upper) {
			Date lowKey = new Date (lower);
			Date highKey = new Date (upper);
			
			int qmonth = 4; // April Logs only
			if (lowKey.year == highKey.year &&
				lowKey.month <= qmonth &&
				qmonth <= highKey.month)
				return true;
			
			if (lowKey.year == (highKey.year - 1) &&
				lowKey.month <= qmonth)
				return true;
			
			if (lowKey.year == (highKey.year - 1) &&
				qmonth <= highKey.month)
				return true;
				
			return false;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count fs");
		job.setJarByClass(DateExample_Month.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(IsValidKeyFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
