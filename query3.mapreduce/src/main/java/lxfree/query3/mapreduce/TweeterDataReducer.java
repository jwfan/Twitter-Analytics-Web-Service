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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TweeterDataReducer {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
//		 File file = new File("output");
//		 File outputfile = new File("output2");
		try {
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
//			 br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
//			 out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputfile), StandardCharsets.UTF_8), true);
			String input;
			String text = null;
			String hashid = null;
			String currenthashid = null;
			String[] keyText = null;
			String[] hashidArr = null;
			Map<String, Integer> keyWords = new HashMap<String, Integer>();
			BigInteger id = BigInteger.ONE;
			while ((input = br.readLine()) != null) {
				String[] parts = input.split("\t");
				text = parts[1];
				hashid = parts[0];
				keyText = text.split(",");
				if (currenthashid != null && currenthashid.equals(hashid)) {
					for (String a : keyText) {
						if (keyWords.containsKey(a)) {
							keyWords.put(a, keyWords.get(a) + 1);
						} else {
							keyWords.put(a, 1);
						}
					}
				} else {
					if (currenthashid != null && !currenthashid.equals(hashid)) {
						hashidArr = currenthashid.split("#");
						String output = id + "\t" + hashidArr[0] + "\t" + hashidArr[1] + "\t" + "{";
						id = id.add(BigInteger.ONE);
						for (String a : keyWords.keySet()) {
							output += "\"" + a + "\":" + keyWords.get(a) + ",";
						}
						output = output.substring(0, output.length() - 1) + "}";
						out.write(output + "\n");
						keyWords = new HashMap<String, Integer>();
						for (String a : keyText) {
							if (keyWords.containsKey(a)) {
								keyWords.put(a, keyWords.get(a) + 1);
							} else {
								keyWords.put(a, 1);
							}
						}
						output = "";
					} else {
						for (String a : keyText) {
							if (keyWords.containsKey(a)) {
								keyWords.put(a, keyWords.get(a) + 1);
							} else {
								keyWords.put(a, 1);
							}
						}
					}
					currenthashid = hashid;
				}
			}
			if (currenthashid != null && currenthashid.equals(hashid)) {
				hashidArr = currenthashid.split("#");
				String output = id + "\t" + hashidArr[0] + "\t" + hashidArr[1] + "\t" + "{";
				for (String a : keyWords.keySet()) {
					output += "\"" + a + "\":" + keyWords.get(a) + ",";
				}
				output = output.substring(0, output.length() - 1) + "}";
				out.write(output + "\n");
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
