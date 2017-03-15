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

public final class TweeterDataReducer {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
//		File file = new File("output");
//		File outputfile = new File("output2");
		try {
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
//			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
//			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputfile), StandardCharsets.UTF_8), true);
			String input;
			String text = null;
			String lasttext = null;
			String hashid = null;
			String lasthashid = null;
			String[] keyText = null;
			String[] lastkeyText = null;
			String[] hashidArr = null;
			Map<String, Integer> keyWords = new HashMap<String, Integer>();
			BigInteger id = BigInteger.ONE;
			while ((input = br.readLine()) != null) {
				if (lasthashid == null) {
					String[] parts = input.split("\t");
					lasttext = parts[1];
					lasthashid = parts[0];
					lastkeyText = lasttext.split(",");
					for (String a : lastkeyText) {
						if(keyWords.containsKey(a)) {
							keyWords.put(a, keyWords.get(a)+1);
						} else {
							keyWords.put(a, 1);							
						}
					}
				} else {
					String[] parts = input.split("\t");
					text = parts[1];
					hashid = parts[0];
					keyText = text.split(",");
					if (hashid.equals(lasthashid)) {
						for (String a : keyText) {
							if (keyWords.containsKey(a)) {
								keyWords.put(a, keyWords.get(a) + 1);
							} else {
								keyWords.put(a, 1);
							}
						}
					} else {
						hashidArr = lasthashid.split("#");
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
					}
					lasttext = text;
					lasthashid = hashid;
					lastkeyText = keyText;
				}
			}
			if(lasthashid != null) {
				hashidArr = lasthashid.split("#");
				String output = id + "\t" + hashidArr[0] + "\t" + hashidArr[1] + "\t" + "{";
				for (String a : keyWords.keySet()) {
					output += "\"" + a + "\":" + keyWords.get(a) + ",";
				}
				output = output.substring(0, output.length() - 1) + "}";
				out.write(output + "\n");				
			}
			keyWords.clear();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException ee) {
					ee.printStackTrace();
				}
				if(out != null) {
					out.close();
				}
			}
		}
	}
}
