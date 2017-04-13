package lxfree.query2.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RemoveSame {

	private final static String[] LANG = { "ar", "en", "fr", "in", "pt", "es", "tr" };

	public static class Map extends MapReduceBase implements Mapper {

		@Override
		public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
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
				
				output.collect(new Text(line), new Text(""));
			
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer {
		@Override
		public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
			output.collect(new Text(key.toString()), new Text(""));
			
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(RemoveSame.class);
		conf.setJobName("removesame");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
