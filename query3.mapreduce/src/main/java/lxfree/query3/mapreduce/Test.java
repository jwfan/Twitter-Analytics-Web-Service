package lxfree.query3.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

public class Test {
	
	

	public static void main(String[] args) {
		File f1 = new File("output");
		File f2 = new File("query3_ref.txt");
		BufferedReader br = null;
		Map<String, String> maps = new HashMap<String, String>();
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(f2), StandardCharsets.UTF_8));
			String line = null;
			while((line = br.readLine())!=null) {
				JSONObject jo =  new JSONObject(line);
				String tid = jo.getString("tweet_id");
				//String score = jo.getString("impactScore"); 
				//maps.put(tid, score);
				String censor=jo.getString("censored_text");
				maps.put(tid, censor);
			}
			br.close();
			br = new BufferedReader(new InputStreamReader(new FileInputStream(f1), StandardCharsets.UTF_8));
			while((line = br.readLine())!=null) {
				String tid = line.split("\t")[0];
				//String score = line.split("\t")[4];
				String censor = line.split("\t")[3];
				if(!maps.containsKey(tid) || !maps.get(tid).equals(censor)) {
					System.out.println(tid);
					System.out.println("ref:\t" + maps.get(tid));
					System.out.println("out:\t" + censor);
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(br !=null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

}
