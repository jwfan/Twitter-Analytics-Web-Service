package lxfree.query2.mapreduce;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	private final static String LETTER_REGEX = "\\p{L}+";
	private final static String[] LANG = {"ar","en", "fr", "in", "pt", "es", "tr"};
	private static Map<String, Integer> tIds = new HashMap<String, Integer>();
	private static Map<String, Integer> stopWords = new HashMap<String, Integer>();
	
	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
//		String fileName = System.getenv("mapreduce_map_input_file");
		File file = new File("part-r-00000");
		File output = new File("output");
		
		if(stopWords.size() == 0) {
			InputStream stopfile = TweeterDataMapper.class.getResourceAsStream( "/stopwords.txt" );
			BufferedReader stopbr = null;
			try {
				stopbr = new BufferedReader(new InputStreamReader(stopfile, StandardCharsets.UTF_8));
				String l = null;
				while((l=stopbr.readLine())!=null) {
					stopWords.put(l.toLowerCase(), 1);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if(stopbr!=null) {
					try {
						stopbr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		
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
				
				//split key words
				StringBuilder keyWords = new StringBuilder();
				Pattern p = Pattern.compile(LETTER_REGEX);
				Matcher m = p.matcher(text);
				String keyword;
				while(m.find()) {
					keyword = m.group();
					if(!stopWords.containsKey(keyword.toLowerCase())) {
						keyWords.append(keyword).append(",");						
					}
				}
				if(keyWords.length() == 0) {
					continue;
				}
				//print out valid data
				for(int i = 0; i < hashtags.length(); i++) {
					String hashText = hashtags.getJSONObject(i).getString("text");
					out.write(hashText + "\t" + uid  + "\t" + keyWords.substring(0, keyWords.length() - 1) + "\t" + tid + "\n");
				}
				
			}
		} catch(IOException e) {
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
