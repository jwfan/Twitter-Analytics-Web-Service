package query2;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

public class MySqlServletQuery2 extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Connection conn1;
	private static Connection conn2;
	private static int choose=0;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	// for test
	//private static String TABLENAME = "test_table";
	private static String TABLENAME = "q2_table";
	private final static String regex = "[0-9]+";
	private static Map<String, JSONArray> cache = new HashMap<String, JSONArray>();

	public MySqlServletQuery2() {
		try {
			conn1 = ConnectionManager.getConnection(0);
			conn2 = ConnectionManager.getConnection(1);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		System.out.println("Received Request:" + System.currentTimeMillis());
		final String hashtag = request.getParameter("hashtag");
		final String N = request.getParameter("N");
		final String keywordslist = request.getParameter("list_of_key_words");
		System.out.println("End parsing parameter:" + System.currentTimeMillis());
		response.setStatus(200);
		response.setContentType("text/plain;charset=UTF-8");
		final PrintWriter writer = response.getWriter();

		String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
		// invalid parameter check
		if ("".equals(hashtag) || "".equals(keywordslist) || !N.matches(regex)) {
			writer.write(result);
			writer.close();
		} else {
			/* get n and keywords list from request args */
			int n = Integer.valueOf(N);
			String[] keywords = keywordslist.split(",");
			PriorityQueue<KVPair> pq = new PriorityQueue<KVPair>(11, new Comparator<KVPair>() {
				@Override
				public int compare(KVPair o1, KVPair o2) {
					if (!o1.getValue().equals(o2.getValue())) {
						return o1.getValue() - o2.getValue();// generate the min heap for frequency
					} else {
						if (o2.getKey() > o1.getKey()) {// generate the max heap for id
							return 1;
						} else if (o2.getKey() < o1.getKey()) {
							return -1;
						} else {
							return 0;
						}
					}
				}
			});

			PreparedStatement stmt = null;
			try {
		        if(cache.containsKey(hashtag)) {
		        	JSONArray cacheRes = cache.get(hashtag);
		        	for(int i = 0; i < cacheRes.length(); i++) {
		        		JSONObject jo = cacheRes.getJSONObject(i);
		        		Long userid = jo.getLong("user_id");
		        		JSONObject cacheKW = jo.getJSONObject("keywords");
		        		int score = 0;
						for (int j = 0; j < keywords.length; j++) {
							try {
								score += cacheKW.getInt(keywords[j]);
							} catch (JSONException e) {
								continue;
							}
						}
						KVPair entry = new KVPair(userid, score);
						if (pq.size() < n) {
							pq.add(entry);
						} else {
							KVPair peek = pq.peek();
							if (peek.getValue() < entry.getValue()) {
								pq.poll();
								pq.add(entry);
							} else if (peek.getValue().equals(entry.getValue()) && peek.getKey() > entry.getKey()) {
								pq.poll();
								pq.add(entry);
							}
						}
		        	}
		        } else {
		        	String sql = "SELECT hashtag, user_id, keywords FROM " + TABLENAME + " where hashtag=?";
		        	/* Decide to which database to query */
		        	switch(choose){
		        	case 0:stmt = conn1.prepareStatement(sql);break;
		        	case 1:stmt = conn2.prepareStatement(sql);break;
		        	default:
		        		if(choose%2==0)
		        			stmt=conn1.prepareStatement(sql);
		        		else
		        			stmt=conn2.prepareStatement(sql);
		        	}
		        	choose=(choose+1)%2;
		        	stmt.setString(1, hashtag);
		        	System.out.println("Start query data:" + System.currentTimeMillis());
		        	ResultSet rs = stmt.executeQuery();
		        	System.out.println("End query data:" + System.currentTimeMillis());
		        	JSONArray cacheJa = new JSONArray();
					while (rs.next()) {
						int score = 0;
						Long userid = Long.valueOf(rs.getString("user_id"));
						//get calculated keywords count from database
						JSONObject jo = new JSONObject(rs.getString("keywords"));
						JSONObject cacheObj = new JSONObject();
						cacheObj.put("user_id", userid);
						cacheObj.put("keywords", jo);
						cacheJa.put(cacheObj);
						for (int i = 0; i < keywords.length; i++) {
							try {
								score += jo.getInt(keywords[i]);
							} catch (JSONException e) {
								continue;
							}
						}
						KVPair entry = new KVPair(userid, score);
						if (pq.size() < n) {
							pq.add(entry);
						} else {
							KVPair peek = pq.peek();
							if (peek.getValue() < entry.getValue()) {
								pq.poll();
								pq.add(entry);
							} else if (peek.getValue() == entry.getValue() && peek.getKey() > entry.getKey()) {
								pq.poll();
								pq.add(entry);
							}
						}
					}
					System.out.println("End parsing response of DB:" + System.currentTimeMillis());
		        }
				if (pq.size() > 0) {
					StringBuilder res = new StringBuilder();
					while (pq.peek() != null) {
						KVPair peek = pq.poll();
						String s = peek.getKey() + ":" + peek.getValue() + ",";
						res.insert(0, s);
					}
					result += res.substring(0, res.length() - 1) + "\n";
				}
				System.out.println("Start wwrite result:" + System.currentTimeMillis());
				writer.write(result);
				System.out.println("End write result:" + System.currentTimeMillis());
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

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
