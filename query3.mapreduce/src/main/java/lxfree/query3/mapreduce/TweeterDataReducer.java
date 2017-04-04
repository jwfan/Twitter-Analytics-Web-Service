package lxfree.query3.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

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
		
		
		JSONArray jsonArray = new JSONArray();
		JSONObject tidValue = new JSONObject();
		try {
			File file = new File("output");
			File output = new File("output2");
//			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			while ((input = br.readLine()) != null) {
				String[] parts = input.split("\t");
				tid = parts[0];
				uid = parts[1];
				timestamp = parts[2];
				text = parts[3];
				impact_score = Integer.parseInt(parts[4]);
				wordFreq = parts[5];
				tidValue = new JSONObject();
				String zero13 = "0000000000000";
				String zero19 = "0000000000000000000";
				String timestamp13 = zero13.substring(0, 13 - timestamp.length()) + timestamp;
				String uid19 = zero19.substring(0, 19 - uid.length()) + uid;
				timeuid = timestamp13 + uid19;
				JSONObject tidOb = new JSONObject();
				tidOb.put("text", text);
				tidOb.put("impact_score", impact_score);
				tidOb.put("wordFreq", wordFreq);
				tidValue.put(tid, tidOb);
				if (currenttimeuid != null && currenttimeuid.equals(timeuid)) {
					jsonArray.put(tidValue);
				} else {
					if (currenttimeuid != null && !currenttimeuid.equals(timeuid)) {
						StringBuilder sb = new StringBuilder();
						for(int i = 0; i < jsonArray.length(); i++) {
							JSONObject jo = jsonArray.getJSONObject(i);
							for(String key: jo.keySet()) {
								JSONObject tidObj = jo.getJSONObject(key);
								sb.append("[{\"").append(key).append("\":{");
								sb.append("\"wordFreq\":").append(tidObj.getString("wordFreq")).append(",");
								sb.append("\"impact_score\":").append(tidObj.getInt("impact_score")).append(",");
								sb.append("\"text\":\"").append(tidObj.getString("text")).append("\"");
								sb.append("}}]");
							}
						}
						out.write(currenttimeuid + "\t" + sb + "\n");
						jsonArray=new JSONArray();
						jsonArray.put(tidValue);
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
