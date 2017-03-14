package lxfree.query2.mapreduce;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
/**
 * This class is to serve as mapper in MapReduce.
 * List all the data and count as 1
 * @author Ruixue
 *
 */
public class TweeterDataMapper {
	
	private final static String SHORTURL_REGEX = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
	private final static String[] LANG = {"ar","en", "fr", "in", "pt", "es", "tr"};
	private static Map<String, Integer> tIds = new HashMap<String, Integer>();
	
	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
//		String fileName = System.getenv("mapreduce_map_input_file");
		File file = new File("part-r-00000");
		File output = new File("output");
		try{
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
//			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			String line;
			while((line = br.readLine()) != null) {
				
				/*
				 * 1. Check Malformed Data 
				 */
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
								continue;
							}
						} catch(JSONException e2) {
							continue;
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
								continue;
							}
						} catch(JSONException e2) {
							continue;
						}
					}
					
					//created_at field is missing or empty
					date = jo.getString("created_at");
					if("".equals(date)){
						continue;
					}
					
					//text field is missing or empty
					text = jo.getString("text");
					if("".equals(text)) {
						continue;
					}
					//lang field is missing or empty
					lang = jo.getString("lang");
					if("".equals(lang)) {
						continue;
					}
					
					//hashtag text (stated above) is missing or empty
					JSONObject entities = jo.getJSONObject("entities");
					hashtags = entities.getJSONArray("hashtags");
					if(hashtags.length() == 0) {
						continue;
					}
					
					
				} catch(JSONException e) {
					continue;
				}
				
				/*
				 * 2. Filter out invalid Language of Tweets
				 */
					
				if(!Arrays.asList(LANG).contains(lang)) {
					continue;
				}
					
				/*
				 * 3. Remove Shortened URLs
				 */
				text = text.replaceAll(SHORTURL_REGEX, "");
				
				/*
				 * 4. Remove duplicated tweet id
				 */
				if(tIds.containsKey(tid)) {
					continue;
				} else {
					tIds.put(tid, 1);
				}
				
				//print out valid data
				JSONObject res = new JSONObject();
				JSONArray hasharr = new JSONArray();
				for(int i = 0; i < hashtags.length(); i++) {
//					out.write(hashtags.getJSONObject(i).getString("text") + "\t" + uid + "\t" + text + "\n");
//					hasharr.put(hashtags.getJSONObject(i).getString("text"));
				}
//				res.put("hashtag_text", hasharr);
//				res.put("text", text);
//				res.put("lang", lang);
//				res.put("userid", uid);
//				res.put("tid", tid);
//				out.write(res.toString()+ " ");
//				out.write(tid + "\t" + uid  + "\t" + "\n");
			}
		} catch(Exception e) {
			e.printStackTrace();
		} 
		finally {
			if(br!=null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(out != null) {
				out.close();
			}
		}
	}

}
