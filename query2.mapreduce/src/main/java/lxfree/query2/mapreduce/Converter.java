package lxfree.query2.mapreduce;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

public class Converter {

	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		String fileName = args[0];
		File file = new File(fileName);
		File output = new File("output");
		Map<String, String> map = new HashMap<String, String>();
		
		 try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8), true);
			String line = null;
			while((line = br.readLine())!=null) {
				String[] sArr = line.split("\t");
				JSONObject jo = new JSONObject();
				jo.put(sArr[2], sArr[3]);
				if(!map.containsKey(sArr[1])) {
					map.put(sArr[1], jo.toString());
				} else {
					map.put(sArr[1], map.get(sArr[1])+jo.toString());
				}
			}
			
			for(Entry<String, String> e: map.entrySet()) {
				out.write(e.getKey() + "\t" + e.getValue() + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
	}

}
