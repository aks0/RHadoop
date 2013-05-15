//package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RHadoopIndexGen {

	public static class RMapper extends Mapper<Object, Text, Text, Text> {
   
		private Text rvalue = new Text();
		private Text rkey = new Text ();
		Log LOG = LogFactory.getLog (Mapper.class);

		public void map(Object key, Text value, Context context
	                   ) throws IOException, InterruptedException {
			String strValue = value.toString().trim();
			int keyIndex = strValue.indexOf("\t");
			if (keyIndex == -1) return;
			
			rkey.set(strValue.substring(0, keyIndex));
			rvalue.set(strValue.substring(keyIndex).trim());
			context.write(rkey, rvalue);
	   }
	 }
	 
	 public static class RReducer 
	      extends Reducer<Text, Text, Text, Text> {
		
		Log LOG = LogFactory.getLog (Reducer.class);
	
	   public void reduce(Text key, Iterable<Text> values, Context context)
			   throws IOException, InterruptedException {
	     super.reduce(key, values, context);
	     LOG.fatal ("^ ReducerJob: " + context.getTaskAttemptID() +
	    		 	", KeyWritten: " + key.toString()
	    		 );
	   }
	 }

	@InterfaceAudience.Public
	@InterfaceStability.Stable
	public static class RIndexPartitioner extends Partitioner<Text, Text> {

		public RIndexPartitioner () {
		}
		
		public int getPartition(Text key, Text value, int numReduceTasks) {
			System.out.println ("======RHadoop's Partitioner");
			return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
		
		public void configure (JobConf arg0) {
	 
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws 	IllegalArgumentException,
													IOException,
													ClassNotFoundException,
													InterruptedException {
	    Configuration conf = new Configuration();
	    String[] otherArgs =
	    			new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: rindexgen <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "r-index gen");
	    job.setJarByClass(RHadoopIndexGen.class);
	    job.setMapperClass(RMapper.class);
	    job.setCombinerClass(RReducer.class);
	    job.setReducerClass(RReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setPartitionerClass(RIndexPartitioner.class);
	    
	    System.out.println ("======RHadoop's Job");
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
