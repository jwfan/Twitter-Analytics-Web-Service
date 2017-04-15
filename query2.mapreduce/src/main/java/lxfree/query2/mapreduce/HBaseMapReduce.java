package lxfree.query2.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseMapReduce {

	private static String zkAddr = "172.31.58.59";
	
	/**
	 *	Mapper for habase, list hashtag, userid, keywords count
	 */
	public static class KeywordsMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text keyWord = new Text();
		private Text valueWord = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] pair = line.split("\t");
			String hashtag = pair[0];
			String uid = pair[1];
			String keywords = pair[2];

			keyWord.set(hashtag);
			valueWord.set(uid + "\t" + keywords);
			context.write(keyWord, valueWord);
		}
	}
	
	/**
	 *	Reducer to insert the hashtag, userid and keywords count
	 */
	public static class KeyWordsReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(Bytes.toBytes(key.toString()));
			for (Text val : values) {
				String[] valArr = val.toString().split("\t");
				String uid = valArr[0];
				String keyword = valArr[1];
				put.addColumn(Bytes.toBytes("data"),Bytes.toBytes(uid),Bytes.toBytes(keyword));
			}
			context.write(new ImmutableBytesWritable(key.getBytes()), put);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
	    
	    Job job = Job.getInstance(conf, "query2Habase");
	    job.setJarByClass(HBaseMapReduce.class);
	    job.setMapperClass(KeywordsMapper.class);
	    job.setReducerClass(KeyWordsReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    TableMapReduceUtil.initTableReducerJob("query2Habase", KeyWordsReducer.class, job);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
