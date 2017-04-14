package lxfree.query3.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONObject;

public final class TweeterDataReducer {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		String input;
//		String tid;
//		String text;
//		int impact_score;
//		String wordFreq;
//		String currenttimeuid = null;
//		String timeuid = null;
//		
//		
//		JSONArray jsonArray = new JSONArray();
//		JSONObject tidValue = new JSONObject();
		try {
			File file = new File("output");
//			File file = new File("test");
			File output = new File("output2");
//			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			while ((input = br.readLine()) != null) {
				out.write(input + "\n");
//				String[] parts = input.split("\t");
//				timeuid = parts[0];
//				tid = parts[1];
//				text = parts[2];
//				impact_score = Integer.parseInt(parts[3]);
//				wordFreq = parts[4];
//				tidValue = new JSONObject();
//				JSONObject tidOb = new JSONObject();
//				tidOb.put("text", new JSONObject(text).get("censored_text"));
//				tidOb.put("impact_score", impact_score);
//				tidOb.put("wordFreq", wordFreq);
//				tidValue.put(tid, tidOb);
//				if (currenttimeuid != null && currenttimeuid.equals(timeuid)) {
//					jsonArray.put(tidValue);
//				} else {
//					if (currenttimeuid != null && !currenttimeuid.equals(timeuid)) {
//						out.write(currenttimeuid + "\t" + jsonArray.toString() + "\n");
//						jsonArray=new JSONArray();
//						jsonArray.put(tidValue);
//					} else {
//						jsonArray.put(tidValue);
//					}
//					currenttimeuid = timeuid;
//				}
//			}
//			if (currenttimeuid != null && currenttimeuid.equals(timeuid)) {
//				jsonArray.put(tidValue);
//				out.write(currenttimeuid + "\t" + jsonArray.toString() + "\n");
//			}
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
