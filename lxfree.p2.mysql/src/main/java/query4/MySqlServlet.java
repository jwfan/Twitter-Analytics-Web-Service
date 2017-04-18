package query4;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class MySqlServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static Connection conn1;
	private static Connection conn2;
	// private static Connection conn3;
	private static int choose = 0;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	// for test
	// private static String TABLENAME = "test_table";
	private static String TABLENAME = "q4_table";
	private final static String regex = "[0-9]+";
	private static Map<String, JSONArray> cache = new HashMap<String, JSONArray>();

	public MySqlServlet() {
		try {
			conn1 = ConnectionManager.getMySqlConnections();
			// System.out.println("Connect to database 1 done.");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		final String operation = request.getParameter("op");
		final String field = request.getParameter("field");
		final String tid1 = request.getParameter("tid1");
		final String tid2 = request.getParameter("tid2");
		final String payload = request.getParameter("payload");
		final String uuid = request.getParameter("uuid");
		final Integer seq = Integer.valueOf(request.getParameter("seq"));
		response.setStatus(200);
		response.setContentType("text/plain;charset=UTF-8");
		final PrintWriter writer = response.getWriter();
		ConcurrentHashMap<String, PriorityBlockingQueue<Integer>> readMap = new ConcurrentHashMap<String, PriorityBlockingQueue<Integer>>();
		final ConcurrentHashMap<String, PriorityBlockingQueue<Integer>> opMap = new ConcurrentHashMap<String, PriorityBlockingQueue<Integer>>();
		final Map<String, Integer> seqMap = new HashMap<String, Integer>();
		// final Map<String, PriorityQueue<KVPair>> opwaitMap = new
		// HashMap<String, PriorityQueue<KVPair>>();
		String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";

		// READ
		if ("read".equals(operation)) {
			// invalid parameter check
			if (!"".equals(operation) || !"".equals(payload)) {
				writer.write(result);
				writer.close();
			} else {
				if (!readMap.containsKey(uuid)) {
					readMap.put(uuid, new PriorityBlockingQueue<Integer>());
				}
				readMap.get(uuid).add(seq);
				Thread t = new Thread(new Runnable() {
					public void run() {
						// synchronized (opMap.get(uuid)) {
						while (opMap.containsKey(uuid) && opMap.get(uuid).peek() < seq && seqMap.containsKey(uuid)
								&& seqMap.get(uuid) < seq) {
							try {
								Thread.sleep(0);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						// querytfavorite_count,
						Connection conn = null;
						PreparedStatement stmt = null;
						try {
							String sql = "SELECT twitter_id, timestamp, tuid, tusername,ttext,tfavorite_count,tretweet_count FROM "
									+ TABLENAME + " WHERE and twitter_id>=? and twitter_id<=?";
							stmt = conn.prepareStatement(sql);
							stmt.setString(1, tid1);
							stmt.setString(2, tid2);
							// remove replicated tweets
							ResultSet rs = stmt.executeQuery();
							String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
							while (rs.next()) {
								String tweetId = rs.getString("twitter_id");
								String tuid = rs.getString("tuid");
								String tusername = rs.getString("tusername");
								String ttext = rs.getString("ttext");
								Long timestamp = rs.getLong("timestamp");
								int tfavoriteCount = rs.getInt("tfavorite_count");
								int tretweetCount = rs.getInt("tretweet_count");
								result += tweetId + "\\" + timestamp + "\\" + tuid + "\\" + tusername + "\\" + ttext
										+ "\\" + tfavoriteCount + "\\" + tretweetCount + "\n";
							}
							writer.write(result);
							writer.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						// opwaitMap.get(uuid).poll();
						opMap.get(uuid).notifyAll();
						// }
					}
				});
				t.start();
			}
		}
		// WRITE
		if ("write".equals(operation))

		{
			// invalid parameter check
			final String tid;
			final String uid;
			final String username;
			final String date;
			final String text;
			final int favorite_count;
			final int retweet_count;
			final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy");
			if (!"".equals(field) || !"".equals(tid1) || !"".equals(tid2)) {
				// System.out.println("Malformed!");
			} else {
				if (!opMap.containsKey(uuid)) {
					opMap.put(uuid, new PriorityBlockingQueue<Integer>());
				}
				opMap.get(uuid).add(seq);
				if (!seqMap.containsKey(uuid)) {
					seqMap.put(uuid, opMap.get(uuid).peek());
				}
				JSONObject jo = new JSONObject(payload);
				tid = jo.get("id").toString();
				uid = jo.getJSONObject("user").get("id").toString();
				username = jo.getJSONObject("user").get("screen_name").toString();
				date = jo.getString("created_at");
				text = jo.getString("text");
				favorite_count = jo.getInt("favorite_count");
				retweet_count = jo.getInt("retweet_count");
				Thread t = new Thread(new Runnable() {
					public void run() {
						synchronized (opMap.get(uuid)) {
							while (opMap.containsKey(uuid) && opMap.get(uuid).peek() == seqMap.get(uuid)) {
								try {
									opMap.get(uuid).wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							// querytfavorite_count,
							Long timestamp = null;
							try {
								timestamp = dateFormat.parse(date).getTime() / 1000l;
							} catch (ParseException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
							Connection conn = null;
							PreparedStatement stmt = null;
							try {
								//String sql = "INSERT INTO" + TABLENAME
								//		+ " (twitter_id, timestamp, tuid, tusername,ttext,tfavorite_count,tretweet_count) "
								//		+ " VALUES( '?','?','?','?','?','?','?')";
								String fieldsterminated = "\t\t";  
						        String linesterminated = "\t\r\n";  
						        String sql = "LOAD DATA LOCAL INFILE 'sql.csv' INTO TABLE"+TABLENAME+" FIELDS TERMINATED BY '"  
						                + fieldsterminated + "'  LINES TERMINATED BY '" + linesterminated  
						                + "' (src_userid,target_userid,message,s1,s2,s3,s4) ";  
								stmt = conn.prepareStatement(sql);
								stmt.setString(1, tid);
								stmt.setLong(2, timestamp);
								stmt.setString(3, uid);
								stmt.setString(4, username);
								stmt.setString(5, text);
								stmt.setInt(6, favorite_count);
								stmt.setInt(7, retweet_count);
								// remove replicated tweets
								ResultSet rs = stmt.executeQuery();
								String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
								result += "success\n";
								writer.write(result);
								writer.close();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							opMap.get(uuid).poll();
							seqMap.put(uuid, opMap.get(uuid).peek());
							opMap.get(uuid).notifyAll();
						}
					}
				});
				t.start();
			}
		}
		// SET
		if ("set".equals(operation)) {
			// invalid parameter check
			if (!"".equals(tid2)) {
				// System.out.println("Malformed!");
			} else {
				if (!opMap.containsKey(uuid)) {
					opMap.put(uuid, new PriorityBlockingQueue<Integer>());
				}
				opMap.get(uuid).add(seq);
				if (!seqMap.containsKey(uuid)) {
					seqMap.put(uuid, opMap.get(uuid).peek());
				}
				Thread t = new Thread(new Runnable() {
					public void run() {
						synchronized (opMap.get(uuid)) {
							while (opMap.containsKey(uuid) && opMap.get(uuid).peek() == seqMap.get(uuid)) {
								try {
									opMap.get(uuid).wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							// querytfavorite_count,
							Integer count;
							Connection conn = null;
							PreparedStatement stmt = null;
							try {
								String sql = "UPDATE" + TABLENAME + "SET" + field + "='?' WHERE twitter_id='?'";
								stmt = conn.prepareStatement(sql);
								try {
									count = Integer.parseInt(payload);
									stmt.setInt(1, count);
								} catch (Exception e) {
									// TODO: handle exception
									stmt.setString(1, payload);
								}
								stmt.setString(2, tid1);
								// remove replicated tweets
								ResultSet rs = stmt.executeQuery();
								String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
								result += "success\n";
								writer.write(result);
								writer.close();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							opMap.get(uuid).poll();
							seqMap.put(uuid, opMap.get(uuid).peek());
							opMap.get(uuid).notifyAll();
						}
					}
				});
				t.start();
			}
		}
		// DELETE
		if ("delete".equals(operation)) {
			// invalid parameter check
			if (!"".equals(field) || !"".equals(tid2) || !"".equals(payload)) {
				// System.out.println("Malformed!");
			} else {
				if (!opMap.containsKey(uuid)) {
					opMap.put(uuid, new PriorityBlockingQueue<Integer>());
				}
				opMap.get(uuid).add(seq);
				if (!seqMap.containsKey(uuid)) {
					seqMap.put(uuid, opMap.get(uuid).peek());
				}
				Thread t = new Thread(new Runnable() {
					public void run() {
						synchronized (opMap.get(uuid)) {
							while (opMap.containsKey(uuid) && opMap.get(uuid).peek() == seqMap.get(uuid)) {
								try {
									opMap.get(uuid).wait();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							Connection conn = null;
							PreparedStatement stmt = null;
							try {
								String sql = "DELETE FROM" + TABLENAME + "WHERE twitter_id='?'";
								stmt = conn.prepareStatement(sql);
								stmt.setString(1, tid1);
								ResultSet rs = stmt.executeQuery();
								String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
								result += "success\n";
								writer.write(result);
								writer.close();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							opMap.get(uuid).poll();
							seqMap.put(uuid, opMap.get(uuid).peek());
							opMap.get(uuid).notifyAll();
						}
					}
				});
				t.start();
			}
		}

	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
