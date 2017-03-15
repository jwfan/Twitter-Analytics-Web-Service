package lxfree.query3.mapreduce;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class is to serve as mapper in MapReduce. List all the data and count as
 * 1
 * 
 * @author Ruixue
 *
 */
public class TweeterDataMapper {

	private final static String SHORTURL_REGEX = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
	private final static Pattern NO_LETTER_REGEX = Pattern.compile("['\\-0-9]+");
	private final static Pattern LETTER_REGEX = Pattern.compile("[A-Za-z0-9'\\-?]+");
	private final static String LANG = "en";
	private static Map<String, Integer> tIds = new HashMap<String, Integer>();
	private static Map<String, Integer> countMap = new HashMap<String, Integer>();
	private static Map<String, Integer> stopWords = new HashMap<String, Integer>();

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		// String fileName = System.getenv("mapreduce_map_input_file");
		 File file = new File("part-r-00000");
		 File output = new File("output");
		 // Load stop words
		if (stopWords.size() == 0) {
			InputStream stopfile = TweeterDataMapper.class.getResourceAsStream("/stopwords.txt");
			BufferedReader stopbr = null;
			try {
				stopbr = new BufferedReader(new InputStreamReader(stopfile, StandardCharsets.UTF_8));
				String l = null;
				while ((l = stopbr.readLine()) != null) {
					stopWords.put(l.toLowerCase(), 1);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (stopbr != null) {
					try {
						stopbr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		try {
			 br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			 out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
//			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);

			String line;
			while ((line = br.readLine()) != null) {

				/*
				 * 1. Check Malformed Data
				 */
				JSONObject jo = null;
				String tid;
				String uid;
				String date;
				String lang;
				String text;
				long time;
				int favorite_count;
				int retweet_count;
				int followers_count;
				try {
					// Convert to json object
					jo = new JSONObject(line);

					// Both id and id_str of the tweet object are missing or
					// empty
					try {
						tid = jo.get("id").toString();
					} catch (JSONException e1) {
						try {
							tid = jo.getString("id_str");
							if (tid.equals("")) {
								continue;
							}
						} catch (JSONException e2) {
							continue;
						}
					}

					// Both id and id_str in user object are missing or empty
					JSONObject user = jo.getJSONObject("user");
					try {
						uid = user.get("id").toString();
					} catch (JSONException e1) {
						try {
							uid = user.getString("id_str");
							if ("".equals(uid)) {
								continue;
							}
						} catch (JSONException e2) {
							continue;
						}
					}

					// created_at field is missing or empty
					date = jo.getString("created_at");
					if ("".equals(date)) {
						continue;
					} else {
						// change date to unix timestamp
						time = System.currentTimeMillis() / 1000L;
					}

					// text field is missing or empty
					text = jo.getString("text");
					if ("".equals(text)) {
						continue;
					}

					// lang field is missing or not equal to en
					lang = jo.getString("lang");
					if ("".equals(lang)) {
						continue;
					}

					// get favorite, retweet and followers count
					favorite_count = jo.getInt("favorite_count");
					retweet_count = jo.getInt("retweet_count");
					followers_count = user.getInt("followers_count");

				} catch (JSONException e) {
					continue;
				}

				/*
				 * 2. Filter out invalid Language of Tweets
				 */

				if (!LANG.equals(lang)) {
					continue;
				}

				/*
				 * 3. Remove Shortened URLs
				 */
				String shortText = text.replaceAll(SHORTURL_REGEX, "");

				/*
				 * 4. Remove duplicated tweet id
				 */
				if (tIds.containsKey(tid)) {
					continue;
				} else {
					tIds.put(tid, 1);
				}

				// split key words and calculate the frequency
				int EWC = 0;
				StringBuilder keyWords = new StringBuilder();
				Matcher letterMatcher = LETTER_REGEX.matcher(shortText);
				String group = "";
				countMap = new HashMap<String, Integer>();
				while (letterMatcher.find()) {
					group = letterMatcher.group().toLowerCase();
					if (!NO_LETTER_REGEX.matcher(group).matches()) {
						if (!stopWords.containsKey(group)) {
							keyWords.append(group).append(",");
							EWC++;
							if (countMap.containsKey(group)) {
								countMap.put(group, countMap.get(group) + 1);
							} else {
								countMap.put(group, 1);
							}
						}
					}
				}
				if (keyWords.length() == 0) {
					continue;
				}

				// calculate impact score
				int impact_score = EWC * (favorite_count + retweet_count + followers_count);
				if (impact_score < 0) {
					impact_score = 0;					
				}
				
				// censor text
				
				
				text = text.replaceAll("\n", "\\n");
				// print out valid data
				String wordFreq = "";
				for (String x : countMap.keySet()) {
					wordFreq += "\"" + x + "\":" + countMap.get(x) + ",";
				}
				out.write(tid + "\t" + uid + "\t" + time + "\t" + text + "\t" + impact_score + "\t{"
						+ wordFreq.substring(0, wordFreq.length() - 1) + "}\n");

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (out != null) {
				out.close();
			}
		}
	}

}
