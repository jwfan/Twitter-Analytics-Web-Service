package lxfree.query2.backend;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import org.json.JSONArray;
import org.json.JSONException;

public class MySqlServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static Connection conn;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "twitter";
	private final static String regex = "[0-9]+";

    public MySqlServlet() {
        try {
			conn = ConnectionManager.getConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        final String hashtag = request.getParameter("hashtag");
        final String N = request.getParameter("N");
        final String keywordslist = request.getParameter("list_of_key_words");
        final PrintWriter writer = response.getWriter();
        response.setStatus(200);
        response.setContentType("text/plain;charset=UTF-8");
        
        Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
				//invalid parameter check
				if("".equals(hashtag) || "".equals(keywordslist) || !N.matches(regex)) {
					writer.write(result);
					writer.close();
				} else {
					int n = Integer.valueOf(N);
					String[] keywords = keywordslist.split(",");
					PriorityQueue<KVPair> pq = new PriorityQueue<KVPair>(new Comparator<KVPair>(){
						@Override
						public int compare(KVPair o1, KVPair o2) {
							if(o1.getValue() != o2.getValue()) {
								return o1.getValue() - o2.getValue();//generate the min heap for frequency
							} else {
								if(o2.getKey() > o1.getKey()) {// generate the max heap for id
									return 1;
								} else if(o2.getKey() < o1.getKey()) {
									return -1;
								} else {
									return 0;
								}
							}
						}
					});
					
					PreparedStatement stmt = null;
					try {
						
						String sql = "SELECT hashtag, user_id, keywords FROM " + TABLENAME + " where hashtag=?";
						stmt = conn.prepareStatement(sql);
						stmt.setString(1, hashtag);
						ResultSet rs = stmt.executeQuery(sql);
						while(rs.next()){
							int score = 0;
							Long userid = Long.valueOf(rs.getString("user_id"));
							JSONObject jo = new JSONObject(rs.getString("keywords"));
							for(int i = 0; i < keywords.length; i++) {
								try{
									score += jo.getInt(keywords[i]);
								} catch (JSONException e) {
									continue;
								}
							}
							KVPair entry = new KVPair(userid, score);
							if(pq.size() < n) {
								pq.add(entry);
							} else {
								KVPair peek = pq.peek();
								if(peek.getValue() < entry.getValue()) {
									pq.poll();
									pq.add(entry);
								} else if(peek.getValue() == entry.getValue() && peek.getKey() > entry.getKey()) {
									pq.poll();
									pq.add(entry);
								}
							}
						}
						if(pq.size() > 0) {
							StringBuilder res = new StringBuilder();
							for(KVPair pair: pq) {
								res.append(pair.getKey()).append(":").append(pair.getValue()).append(",");
							}
							
							result += res.substring(0, res.length() - 1) + "\n";
						}
						writer.write(result);
						writer.close();
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
			}
        });
        t.start();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
