package Lxfreee.teamProj_p1q3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class p1q3_Reducer {
	static List<String> outList = new ArrayList<String>();

	public static void main(String[] args) {
		File file = new File("test");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			String input;
			//String hashtag = null;
			//String lasthashtag = null;
			//List<String> hashtagText = new ArrayList<String>();
			//List<String> lasthashtagText = new ArrayList<String>();
			String text = null;
			String lasttext = null;
			String hashid = null;
			String lasthashid = null;
			//String tid = null;
			//String lasttid = null;
			String[] keyText = null;
			String[] lastkeyText = null;
			Map<String, Integer> keyWords = new HashMap<String, Integer>();
			while ((input = br.readLine()) != null) {
				if (lasthashid == null) {
					String[] parts = input.split("\t");
					//lasthashtag = parts[2];
					lasttext = parts[1];
					lasthashid = parts[0];
					//lasttid = parts[0];
					lastkeyText = lasttext.split(",");
					//lasthashtagText.addAll(Arrays.asList(lasthashtag.split(",")));
					for (String a : lastkeyText) {
						keyWords.put(a, 1);
					}
				} else {
					String[] parts = input.split("\t");
					//hashtag = parts[2];
					text = parts[1];
					hashid = parts[0];
					//tid = parts[0];
					keyText = text.split(",");
					//hashtagText.addAll(Arrays.asList(hashtag.split(",")));
					if (hashid.equals(lasthashid)) {
						//hashtagText.removeAll(lasthashtagText);
						//hashtagText.addAll(lasthashtagText);
						for (String a : keyText) {
							if (keyWords.containsKey(a)) {
								keyWords.put(a, keyWords.get(a) + 1);
							}
							else{
								keyWords.put(a,  1);
							}
						}
					} else {
						String output = lasthashid + "\t" ;
						/*for (String a : lasthashtagText) {
							output += a + ",";
						}
						output = output.substring(0, output.length() - 1);
						output += "\t";*/
						for (String a : keyWords.keySet()) {
							output += a + ":" + keyWords.get(a) + ",";
						}
						System.out.println(output.substring(0, output.length() - 1));
						keyWords.clear();
						for (String a : keyText) {
							keyWords.put(a, 1);
						}
						output = "";
					}
					//lasthashtag = hashtag;
					lasttext = text;
					lasthashid = hashid;
					//lasttid = tid;
					lastkeyText = keyText;
					//lasthashtagText = new ArrayList<String>(hashtagText);
					//hashtagText.clear();
				}

				//
			}
			String output = lasthashid + "\t" ;
			/*for (String a : lasthashtagText) {
				output += a + ",";
			}
			output = output.substring(0, output.length() - 1);
			output += "\t";*/
			for (String a : keyWords.keySet()) {
				output += a + ":" + keyWords.get(a) + ",";
			}
			System.out.println(output.substring(0, output.length() - 1));
			keyWords.clear();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException ee) {
					ee.printStackTrace();
				}
			}
		}

	}
}
