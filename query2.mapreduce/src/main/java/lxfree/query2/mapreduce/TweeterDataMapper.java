package lxfree.query2.mapreduce;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		String fileName = System.getenv("mapreduce_map_input_file");
		try{
			
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			String line;
			while((line = br.readLine()) != null) {
				
				/*
				 * 1. Check Malformed Data 
				 */
				JSONObject jo = null;
				String id_str;
				String text;
				String hashtag;
				try{
					//Convert to json object
					jo = new JSONObject(line);
					
					//Both id and id_str of the tweet object are missing or empty
					
					
					//Both id and id_str in user object are missing or empty
					
					//created_at field is missing or empty
					
					//text field is missing or empty
					
					//lang field is missing or empty
					
					//hashtag text (stated above) is missing or empty
				} catch(JSONException e) {
					continue;
				}
				
					
				
				/*
				 * 2. Filter out invalid Language of Tweets
				 */
					
					
				/*
				 * 3. Remove Shortened URLs
				 */
				
				/*
				 * 4. Remove duplicated tweet id
				 */
				
				//
				
				
				int countViews = Integer.valueOf(data[2]);
				out.print(pageTitle + "\t" + date  + "\t" + countViews + "\n");
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
