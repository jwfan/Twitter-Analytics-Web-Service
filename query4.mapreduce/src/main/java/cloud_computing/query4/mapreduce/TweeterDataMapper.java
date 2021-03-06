package cloud_computing.query4.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

public class TweeterDataMapper {
	
	private final static Pattern CENSOR_LETTER_REGEX=Pattern.compile("[A-Za-z0-9]+");
	private static Map<String, Integer> bannedWords = new HashMap<String, Integer>();
//	private final static String LANG = "en";
	
	
	public static void main(String[] args) {
		
//		File file = new File("part-r-00000");
//		File output = new File("output");
		BufferedReader br = null;
		PrintWriter out = null;
//		String lang = null;
		
		try{
//			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			 // Load banned words which need to be censored
			if (bannedWords.size() == 0) {
				InputStream bannedfile = TweeterDataMapper.class.getResourceAsStream("/banned_words");
				BufferedReader bannedbr = null;
				try {
					bannedbr = new BufferedReader(new InputStreamReader(bannedfile, StandardCharsets.UTF_8));
					String line = null;
					while ((line = bannedbr.readLine()) != null) {
						bannedWords.put(line.toLowerCase(), 1);
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (bannedbr != null) {
						try {
							bannedbr.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			
			String line;
			while ((line = br.readLine()) != null) {

//				JSONObject jo = null;
				String[] str = line.split("\t");
				String tid = str[0];
				String uid = str[1];
				String usernameJo = str[2];
				String date = str[3];
				String textJSON = str[4];
				int favorite_count = Integer.valueOf(str[5]);
				int retweet_count = Integer.valueOf(str[6]);
				
//				try {
//					// Convert to json object
//					jo = new JSONObject(line);
//					// Both id and id_str of the tweet object are missing or empty
//					try {
//						tid = jo.get("id").toString();
//					} catch (JSONException e1) {
//						try {
//							tid = jo.getString("id_str");
//							if (tid.equals("")) {
//								continue;
//							}
//						} catch (JSONException e2) {
//							continue;
//						}
//					}
//					
//					//Username is missing or empty
//					JSONObject user = jo.getJSONObject("user");
//					try {
//						uid = user.get("id").toString();
//					} catch (JSONException e1) {
//						try {
//							uid = user.getString("id_str");
//							if ("".equals(uid)) {
//								continue;
//							}
//						} catch (JSONException e2) {
//							continue;
//						}
//					}
//					
//					try {
//						username = user.get("screen_name").toString();
//					} catch (JSONException e1) {
//						continue;
//					}
//					// created_at field is missing or empty
//					date = jo.getString("created_at");
//					if ("".equals(date)) {
//						continue;
//					}
//					// text field is missing or empty
//					text = jo.getString("text");
//					if ("".equals(text)) {
//						continue;
//					}
//					
//					// lang field is missing or not equal to en
//					lang = jo.getString("lang");
//					if (!LANG.equals(lang)) {
//						continue;
//					}
					
//					favorite_count = jo.getInt("favorite_count");
//					retweet_count = jo.getInt("retweet_count");
					
//				} catch(JSONException e) {
//					continue;
//				}
				
				// censor text
				String text = new JSONObject(textJSON).getString("text");
				Matcher censorWordMatcher=CENSOR_LETTER_REGEX.matcher(text);
				String group="";
				while(censorWordMatcher.find()){
					group=censorWordMatcher.group().toLowerCase();
					if(bannedWords.containsKey(group)){
						// if banned words are found
						int start=censorWordMatcher.start()+1;
						int end=censorWordMatcher.end()-1;
						String censor=new String();
						for(int i=start;i<end;i++)
							censor+="*";
						text=text.substring(0, start) + censor + text.substring(end);
					}
				}
				JSONObject textJo = new JSONObject();
				textJo.put("censored_text", text);
				
				// censor username
				String username = new JSONObject(usernameJo).getString("username");
				censorWordMatcher=CENSOR_LETTER_REGEX.matcher(username);
				group = "";
				while(censorWordMatcher.find()){
					group=censorWordMatcher.group().toLowerCase();
					if(bannedWords.containsKey(group)){
						// if banned words are found
						int start=censorWordMatcher.start()+1;
						int end=censorWordMatcher.end()-1;
						String censor=new String();
						for(int i=start;i<end;i++)
							censor+="*";
						username=username.substring(0, start) + censor + username.substring(end);
					}
				}
				JSONObject usernamejob = new JSONObject();
				usernamejob.put("username", username);
				out.write(tid + "\t" + date + "\t" + uid + "\t" + usernamejob 
						+ "\t" + textJo.toString() + "\t" + favorite_count + "\t" + retweet_count + "\n");
				
			}
		} catch(IOException e) {
			
		}finally{
			if(br!=null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(out!=null) {
				out.close();
			}
			
		}
		
		
		
		
		
		
	}

}
