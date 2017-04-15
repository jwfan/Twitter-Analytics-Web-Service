package lxfree.query2.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RemoveSame {

	private final static String[] LANG = { "ar", "en", "fr", "in", "pt", "es", "tr" };

	public static class Map  extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			JSONObject jo = null;
			String tid;
			String uid;
			String date;
			String lang;
			String text;
			JSONArray hashtags;
			try{
				//Convert to json object
				jo = new JSONObject(line);
				
				//Both id and id_str of the tweet object are missing or empty
				try{
					tid = jo.get("id").toString();
				} catch (JSONException e1) {
					try {
						tid = jo.getString("id_str");
						if(tid.equals("")){
							return;
						}
					} catch(JSONException e2) {
						return;
					}
				}
				
				//Both id and id_str in user object are missing or empty
				JSONObject user = jo.getJSONObject("user");
				try{
					uid = user.get("id").toString();
				} catch (JSONException e1) {
					try {
						uid = user.getString("id_str");
						if("".equals(uid)){
							return;
						}
					} catch(JSONException e2) {
						return;
					}
				}
				//created_at field is missing or empty
				date = jo.getString("created_at");
				if("".equals(date)){
					return;
				}
				
				//text field is missing or empty
				text = jo.getString("text");
				if("".equals(text)) {
					return;
				}
				
				//lang field is missing or empty
				lang = jo.getString("lang");
				if("".equals(lang)) {
					return;
				}
				
				//hashtag text (stated above) is missing or empty
				JSONObject entities = jo.getJSONObject("entities");
				hashtags = entities.getJSONArray("hashtags");
				if(hashtags.length() == 0) {
					return;
				}
				

				}catch(JSONException e) {
					return;
				}
				/*
				 * 2. Filter out invalid Language of Tweets
				 */
					
				if(!Arrays.asList(LANG).contains(lang)) {
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
		Job job = Job.getInstance(conf, "remove same");
		job.setJarByClass(RemoveSame.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
