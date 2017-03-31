package lxfree.query3.backend;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

	private static Connection conn;
	private static java.sql.Connection mySqlconn;
	private static byte[] bColFamily = Bytes.toBytes("data");
	
    public HBaseServlet() {
        try {
			conn = ConnectionManager.getHBaseConnection();
			mySqlconn = ConnectionManager.getConnection();
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

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();

        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */
        Table linksTable = conn.getTable(TableName.valueOf("links"));
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("followers");
        byte[] idCol = Bytes.toBytes("id");
        scan.addColumn(bColFamily, bCol);
        scan.addColumn(bColFamily, idCol);
        scan.setFilter(new PrefixFilter(id.getBytes()));//row_key is username plus row number, filter by row key prefix
        ResultScanner rs = linksTable.getScanner(scan);
        StringBuilder sb = new StringBuilder();
        for (Result r = rs.next(); r != null; r = rs.next()) {
        	String username = Bytes.toString(r.getValue(bColFamily, idCol));
        	if(id.equals(username)) {//check whether the id in Hbase exactly matches id from front end
        		sb.append("'").append(Bytes.toString(r.getValue(bColFamily, bCol))).append("'").append(",");        		
        	}
        }
        rs.close();
        if(sb.length() > 0) {
        	// Retrive id and imageurl from mySql
        	Statement stmt = null;
        	try {
        		stmt = mySqlconn.createStatement();
        		String wherecon = sb.substring(0,sb.length()-1);
        		String sql = "SELECT id, imageurl FROM users WHERE id in (" + wherecon + ") order by binary id, imageurl asc";
        		ResultSet mysqlrs = stmt.executeQuery(sql);
        		String name = "Unauthorized";
        		String imageUrl = "#";
        		while (mysqlrs.next()) {
        			name = mysqlrs.getString("id");
        			imageUrl = mysqlrs.getString("imageurl");
        	        JSONObject follower = new JSONObject();
        			follower.put("name", name);
        			follower.put("profile", imageUrl);
        			followers.put(follower);
        		}
        	} catch (SQLException e) {
        		e.printStackTrace();
        	} finally {
        		if (stmt != null) {
        			try {
        				stmt.close();
        			} catch (SQLException e) {
        				e.printStackTrace();
        			}
        		}
        	}
        	
        }
        result.put("followers", followers);
		PrintWriter writer = response.getWriter();
		writer.write(String.format("returnRes(%s)", result.toString()));
		writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}


