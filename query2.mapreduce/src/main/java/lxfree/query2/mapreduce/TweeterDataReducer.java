package lxfree.query2.mapreduce;

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
import java.util.Map.Entry;

public final class TweeterDataReducer {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		 File file = new File("output");
		 File outputfile = new File("output2");
		try {
//			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
			 br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			 out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputfile), StandardCharsets.UTF_8), true);
			String input;
//			String text = null;
//			String hashid = null;
//			String currenthashid = null;
//			String[] keyText = null;
//			String hashTag = null;
//			String userid = null;
//			Map<String, Integer> keyWords = new HashMap<String, Integer>();
//			Map<String, Map<String, Integer>> userMap = new HashMap<String, Map<String, Integer>>();
			BigInteger id = BigInteger.ONE;
			while ((input = br.readLine()) != null) {
				out.write(input + "\n");
//				String[] parts = input.split("\t");
//				hashid = parts[0];
//				userid = parts[1];
//				text = parts[2];
//				keyText = text.split(",");
//				if (currenthashid != null && currenthashid.equals(hashid)) {
//					if(!userMap.containsKey(userid)) {
//						userMap.put(userid, new HashMap<String, Integer>());
//					}
//					for (String a : keyText) {
//						keyWords = userMap.get(userid);
//						if (keyWords.containsKey(a)) {
//							keyWords.put(a, keyWords.get(a) + 1);
//						} else {
//							keyWords.put(a, 1);
//						}
//						userMap.put(userid, keyWords);
//					}
//				} else {
//					if (currenthashid != null && !currenthashid.equals(hashid)) {
//						for(Entry<String, Map<String, Integer>> e: userMap.entrySet()) {
//							out.write(id + "\t" + currenthashid + "\t" + e.getKey() + "\t" + "{");
//							id = id.add(BigInteger.ONE);
//							int index = 0;
//							for (String a : e.getValue().keySet()) {
//								out.write("\"");
//								out.write(a);
//								out.write("\"");
//								out.write(":");
//								out.write(e.getValue().get(a).toString());
//								index++;
//								if(index == e.getValue().keySet().size()){
//									out.write("}");
//								} else {
//									out.write(",");							
//								}
//							}
//							out.write("\n");					
//						}
//						userMap = new HashMap<String, Map<String, Integer>>();
//						keyWords = new HashMap<String, Integer>();
//						for (String a : keyText) {
//							if (keyWords.containsKey(a)) {
//								keyWords.put(a, keyWords.get(a) + 1);
//							} else {
//								keyWords.put(a, 1);
//							}
//						}
//						userMap.put(userid, keyWords);
//					} else {
//						for (String a : keyText) {
//							if (keyWords.containsKey(a)) {
//								keyWords.put(a, keyWords.get(a) + 1);
//							} else {
//								keyWords.put(a, 1);
//							}
//							userMap.put(userid, keyWords);
//						}
//					}
//					currenthashid = hashid;
//				}
//			}
//			if (currenthashid != null && currenthashid.equals(hashid)) {
//				String output = "";
//				for(Entry<String, Map<String, Integer>> e: userMap.entrySet()) {
//					output = id + "\t" + currenthashid + "\t" + e.getKey() + "\t" + "{";
//					id = id.add(BigInteger.ONE);
//					for (String a : e.getValue().keySet()) {
//						output += "\"" + a + "\":" + e.getValue().get(a) + ",";
//					}
//					output = output.substring(0, output.length() - 1) + "}";
//					out.write(output + "\n");							
//				}
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
