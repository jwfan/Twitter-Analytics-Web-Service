package lxfree.query2.backend;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class Warmup {
 
  private static String QUERY_URL = "http://q2-mysql-elb-test-350282126.us-east-1.elb.amazonaws.com";
  private static String PRAM = "/q1?key=1239793247987948712739187492308012309184023849817397189273981723912221&message=QTGXGTHWEQENWQVKPIRFO";
  
  public static void main(String[] args) {
   int status = 0;
   try {
    // Send POST request with password and candidate parameters
    URL url = new URL(QUERY_URL + PRAM);
    while(true) {
     HttpURLConnection conn = (HttpURLConnection) url.openConnection();
     conn.setRequestMethod("GET");
     conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
     conn.setRequestProperty("charset", "utf-8");
     conn.setRequestProperty("accept", "text/plain");
     
     // get HTTP response
     status = conn.getResponseCode();
      conn.setConnectTimeout(5000);
      conn.disconnect();
    }
   } catch (MalformedURLException e) {
    System.out.println("Error: Invalid URL");
    e.printStackTrace();
   } catch (IOException e) {
    e.printStackTrace();
   }
  }
}