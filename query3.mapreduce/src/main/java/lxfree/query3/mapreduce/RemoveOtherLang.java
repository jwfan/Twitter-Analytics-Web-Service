package lxfree.query3.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

public class RemoveOtherLang {
	
	public final static String LANG = "en";

	public static class Map  extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			JSONObject jo = null;
			String lang = null;
			try{
				//Convert to json object
				jo = new JSONObject(line);

				//lang field is missing or empty
				lang = jo.getString("lang");
				if(LANG.equals(lang)) {
					return;
				}

				}catch(JSONException e) {
					return;
				}
				context.write(new Text(line), one);
		}
	}


	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {}
			context.write(new Text(key.toString()), new Text(""));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "remove other language");
		job.setJarByClass(RemoveOtherLang.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
