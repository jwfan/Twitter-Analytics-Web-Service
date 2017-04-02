package lxfree.query3.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.io.FileOutputStream;

import org.json.JSONArray;
import org.json.JSONObject;

public final class TweeterDataReducer {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		String input;
		String tid;
		String uid;
		String timestamp;
		String text;
		int impact_score;
		String wordFreq;
		String currenttimeuid = null;
		String timeuid = null;
		JSONArray valueArray;
		// Map<String, Integer> timeuidMap = new HashMap<String, Integer>();
		JSONArray jsonArray = new JSONArray();
		JSONObject tidValue = new JSONObject();
		try {
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			while ((input = br.readLine()) != null) {
			out.write(input + "\n");
				tid = parts[0];
				uid = parts[1];
				timestamp = parts[3];
				text = parts[4];
				impact_score = Integer.parseInt(parts[5]);
				wordFreq = parts[6];
				valueArray = new JSONArray();
				tidValue = new JSONObject();
				String zero13 = "0000000000000";
				String zero19 = "0000000000000000000";
				String timestamp13 = zero13.substring(0, 13 - timestamp.length()) + timestamp;
				String uid19 = zero19.substring(0, 19 - uid.length()) + uid;
				timeuid = timestamp13 + uid19;
				JSONObject textOb = new JSONObject();
				textOb.put("text", text);
				valueArray.put(textOb);
				JSONObject impact_scoreOb = new JSONObject();
				impact_scoreOb.put("impact_score", impact_score);
				valueArray.put(impact_scoreOb);
				JSONObject wordFreqOb = new JSONObject();
				wordFreqOb.put("wordFreq", wordFreq);
				valueArray.put(wordFreqOb);
				tidValue.put(tid, valueArray);
				if (currenttimeuid != null && currenttimeuid.equals(timeuid)) {
					jsonArray.put(tidValue);
				} else {
					if (currenttimeuid != null && !currenttimeuid.equals(timeuid)) {
						out.write(currenttimeuid + "\t" + jsonArray + "\n");
						jsonArray = null;
					} else {
						jsonArray.put(tidValue);
					}
					currenttimeuid = timeuid;
				}
			}
			if (currenttimeuid != null && currenttimeuid.equals(timeuid)) {
				jsonArray.put(tidValue);
				out.write(currenttimeuid + "\t" + jsonArray + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException ee) {
					ee.printStackTrace();
				}
				if (out != null) {
					out.close();
				}
			}
		}
	}
}
