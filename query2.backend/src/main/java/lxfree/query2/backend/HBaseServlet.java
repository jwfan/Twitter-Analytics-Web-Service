package lxfree.query2.backend;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.PriorityQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

public class HBaseServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Connection conn;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "twitter";
	private final static String regex = "[0-9]+";
	private static byte[] bColFamily = Bytes.toBytes("data");
	private static Map<String, JSONArray> cache = new HashMap<String, JSONArray>();

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
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
//		String cookie = request.getHeader("cookie");
//		System.out.println("Cookie:" + cookie);
//		int index = cookie.indexOf(";");
//		Long sendTime = Long.valueOf(cookie.substring(index-10,index));
//		System.out.println("Send time:" + sendTime);
//		System.out.println("Recieve:" + System.currentTimeMillis());
		final String hashtag = request.getParameter("hashtag");
		final String N = request.getParameter("N");
		final String keywordslist = request.getParameter("list_of_key_words");
//		System.out.println("Parse request end time:" + System.currentTimeMillis());
		response.setStatus(200);
		response.setContentType("text/plain;charset=UTF-8");
		final PrintWriter writer = response.getWriter();
		String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
		
		if ("".equals(hashtag) || "".equals(keywordslist) || !N.matches(regex)) {
			writer.write(result);
			writer.close();
		} else {
			int n = Integer.valueOf(N);
			String[] keywords = keywordslist.split(",");
			PriorityQueue<KVPair> pq = new PriorityQueue<KVPair>(11, new Comparator<KVPair>() {
				@Override
				public int compare(KVPair o1, KVPair o2) {
					if (o1.getValue() != o2.getValue()) {
						return o1.getValue() - o2.getValue();// generate the min
																// heap for
																// frequency
					} else {
						if (o2.getKey() > o1.getKey()) {// generate the max heap
														// for id
							return 1;
						} else if (o2.getKey() < o1.getKey()) {
							return -1;
						} else {
							return 0;
						}
					}
				}
			});
			// cache to store the hashtag query result
			if (cache.containsKey(hashtag)) {
				JSONArray cacheRes = cache.get(hashtag);
				for (int i = 0; i < cacheRes.length(); i++) {
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
						} else if (peek.getValue() == entry.getValue() && peek.getKey() > entry.getKey()) {
							pq.poll();
							pq.add(entry);
						}
					}

				}
			} else {
				Table linksTable = null;
				try {
					linksTable = conn.getTable(TableName.valueOf(TABLENAME));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				Get get = new Get(Bytes.toBytes(hashtag));
				// add column family
				get.addFamily(bColFamily);
				Result r;
				try {
					r = linksTable.get(get);
					NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(bColFamily);
					// put user-keyword pair into cache
					JSONArray cacheJa = new JSONArray();
					for (Entry<byte[], byte[]> familyEntry : familyMap.entrySet()) {
						// iterator through the whole column family
						Long userId = Long.parseLong(Bytes.toString(familyEntry.getKey()));							
						String wordFreqCount = Bytes.toString(familyEntry.getValue());
						JSONObject	wfob = new JSONObject(wordFreqCount);
						
						JSONObject cacheObj = new JSONObject();
						cacheObj.put("user_id", userId);
						cacheObj.put("keywords", wfob);
						cacheJa.put(cacheObj);
						
						// calculate score
						int score = 0;
						for (int j = 0; j < keywords.length; j++) {
							try {
								score += wfob.getInt(keywords[j].toLowerCase());
							} catch (JSONException e) {
								continue;
							}
						}
						KVPair entry = new KVPair(userId, score);
						// use priority queue to calculate the top n
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
					cache.put(hashtag, cacheJa);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
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
			writer.write(result);
			writer.close();
		}
	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

}
