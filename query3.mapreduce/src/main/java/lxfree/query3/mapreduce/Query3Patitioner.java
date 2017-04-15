package lxfree.query3.mapreduce;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class Query3Patitioner extends Configured implements Tool{

	//Mapper class
	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String content = value.toString();
			String[] values = content.split("\t");
			String id = values[0];//for partition 1-6
			context.write(new Text(id), value);
		}
	}

	// Reducer class
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				String[] str = val.toString().split("\t");
				String tid = str[2];
				String v = str[3] + "\t" + str[4] + "\t" + str[5] + "\t" 
						+ str[6] + "\t" + str[7];
				context.write(new Text(tid), new Text(v));
			}
		}
	}

	// Partitioner class
	public static class CaderPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			int id = Integer.valueOf(key.toString());
			return id;
		}
	}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Query3");
		job.setJarByClass(Query3Patitioner.class);

		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		job.setMapperClass(MapClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set partitioner statement
		job.setPartitionerClass(CaderPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(6);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String ar[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query3Patitioner(), ar);
		System.exit(0);
	}

}
