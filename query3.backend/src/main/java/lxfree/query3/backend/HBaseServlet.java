package lxfree.query3.backend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.CompareFilter;


import org.json.JSONObject;
import org.json.JSONArray;


public class HBaseServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Connection conn;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "twitter";
	private final static String regex = "[0-9]+";
	private static Map<String, Integer> bannedWords = new HashMap<String, Integer>();
	private static byte[] bColFamily = Bytes.toBytes("tweet");
	
    public HBaseServlet() {
        try {
			conn = ConnectionManager.getHBaseConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
    	
        final String startTime = request.getParameter("time_start");
        final String endTime = request.getParameter("time_end");
        final String startUid = request.getParameter("uid_start");
        final String endUid = request.getParameter("uid_end");
        final String maxTopicWords = request.getParameter("n1");
        final String maxTweets = request.getParameter("n2");
        final PrintWriter writer = response.getWriter();
        response.setStatus(200);
        response.setContentType("text/plain;charset=UTF-8");
		 // Load banned words which need to be censored
		if (bannedWords.size() == 0) {
			InputStream bannedfile = MySqlServlet.class.getResourceAsStream("/banned_words");
			BufferedReader bannedbr = null;
			try {
				bannedbr = new BufferedReader(new InputStreamReader(bannedfile, StandardCharsets.UTF_8));
				String line = null;
				while ((line = bannedbr.readLine()) != null) {
					bannedWords.put(line.toLowerCase(), 1);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (bannedbr != null) {
					try {
						bannedbr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
        if(!startTime.matches(regex) || !endTime.matches(regex) || !startUid.matches(regex) 
        		|| !endUid.matches(regex) || !maxTweets.matches(regex) || !maxTopicWords.matches(regex)) {
        	writer.write(result);
        	writer.close();
        }

        Table linksTable = conn.getTable(TableName.valueOf(TABLENAME));
        Scan scan = new Scan();
        byte[] tidCol = Bytes.toBytes("twitter_id");
        byte[] ctCol = Bytes.toBytes("censored_text");
        byte[] isCol = Bytes.toBytes("impact_score");
        byte[] kwCol = Bytes.toBytes("keywords");

        scan.addColumn(bColFamily, tidCol);
        scan.addColumn(bColFamily, ctCol);
        scan.addColumn(bColFamily, isCol);
        scan.addColumn(bColFamily, kwCol);
        
        //TODO: Add filters
        
        ResultScanner rs = linksTable.getScanner(scan);
        StringBuilder sb = new StringBuilder();
        for (Result r = rs.next(); r != null; r = rs.next()) {
        	
        }
        rs.close();
		writer.write(String.format("returnRes(%s)", result.toString()));
		writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}


