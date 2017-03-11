package lxfree.query1.autoscaling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class AutoScaling {
	
	private static String DC_IMAGE = "ami-2fca3739";
	
	public static void autoScaling(String lgDNS, String testlog) throws MalformedURLException, IOException, InterruptedException {
		String url = "http://" + lgDNS + "/log?name=" + testlog;
		System.out.println("Launch Test:" + url);
		String response = "";
		double n = 0;
		long starttime = 0;
		long endtime = 0;
		while(n < 4000) {
			if(endtime - starttime < 100000) {
				Thread.sleep(100000 - (endtime - starttime));		
			}
			response = getLog(url);
			if(response.lastIndexOf("Current rps=") > -1) {
				int startindex = response.lastIndexOf("rps=") + 3;
				String rps = response.substring(startindex);
				n = Double.parseDouble(rps.substring(1, rps.indexOf("]")));
			}
			
			try {
				starttime = System.currentTimeMillis();
				String dcDNS = UndertowStartUp.launchInstance(DC_IMAGE);
				String addDC = "http://" + lgDNS + "/test/horizontal/add?dns=" + dcDNS;
				System.out.println(addDC);
				addDC(addDC);
				System.out.println("Add data center!");
				endtime = System.currentTimeMillis();
			} catch (Exception e) {
				continue;
			}
		}
		
	}
	
	
    private static void addDC(String addDCURL) throws IOException, InterruptedException {
        URL url = new URL(addDCURL);
        int code = 0;
        HttpURLConnection connection = null;
			while(code != 200) {
				try {
					connection = (HttpURLConnection) url.openConnection();
					code = connection.getResponseCode();
					if(code != 200) {
						Thread.sleep(10000);						
					}
				} catch (IOException e) {
					continue;
				}
			}
	}


	private static String getLog(String urlString) throws MalformedURLException, IOException {
        String response = "";
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        String str;
        while ((str = in.readLine()) != null) {
            response += str;
        }
        in.close();
        return response;
    }

}
