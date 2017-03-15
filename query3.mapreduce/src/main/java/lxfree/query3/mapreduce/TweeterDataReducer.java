package lxfree.query3.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public final class TweeterDataReducer {
	
	public static void main(String[] args) {
		BufferedReader br = null;
		PrintWriter out = null;
		String input;
//		 File file = new File("output");
//		 File outputfile = new File("output2");
		try {
		br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
		while ((input = br.readLine()) != null) {
			out.write(input + "\n");
		}
		}catch (IOException e) {
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
