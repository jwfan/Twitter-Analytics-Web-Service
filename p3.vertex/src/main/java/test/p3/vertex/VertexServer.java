package test.p3.vertex;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;

public class VertexServer extends AbstractVerticle {

	private String TEAMID = "LXFreee";
	private String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private String X = "12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140";
	private static Connection conn1;
	private static Connection conn2;
	private static int choose=0;
	private static String TABLENAME = "q2_table";
	private final static String regex = "[0-9]+";
	private static Map<String, JSONArray> cache = new HashMap<String, JSONArray>();
	

	@Override
	public void start(Future<Void> fut) {
		Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));
		Router router = Router.router(vertx);
		try {
			conn1 = ConnectionManager.getConnection(0);
			//System.out.println("Connect to database 1 done.");
			conn2 = ConnectionManager.getConnection(1);
			//System.out.println("Connect to database 2 done.");
			//conn3 = ConnectionManager.getConnection(2);
			//System.out.println("Connect to database 3 done.");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// options.setAcceptBacklog(32767);
		// options.setUsePooledBuffers(true);
		// options.setReceiveBufferSize(4 * 1024);
		
		/**
		 * Query 2
		 */
		router.route("/q1").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			String key = routingContext.request().getParam("key");
			String message = routingContext.request().getParam("message");
			int Xlength = X.length();
			int Ylength = key.length();
			int layers = 0;
			/* Test the validation of key & message */
			layers = (int) Math.floor(Math.sqrt(message.length() * 2));
			if (Ylength > Xlength || layers == 0 || layers * (layers + 1) != message.length() * 2) {
				response.setStatusCode(200).putHeader("content-type", "text/plain;charset=UTF-8").end(String.format("INVALID"));
			} else {
				/*
				 * 1.Caesarify: step secretKey X =
				 * 12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140
				 * Y=key cipherText=Z
				 **/
				int[] keyX = new int[Xlength];
				for (int i = 0; i < X.length(); i++) {
					keyX[i] = Integer.valueOf(X.charAt(i) + "");
				}
				int[] keyY = new int[Ylength];
				for (int i = 0; i < key.length(); i++) {
					keyY[i] = Integer.valueOf(key.charAt(i) + "");
				}
				int Z;
				int sum1 = 0;
				int sum2 = 0;
				for (int i = Ylength - 2; i < Xlength - 1; i++) {
					sum1 += keyX[i];
				}
				for (int i = Ylength - 1; i < Xlength; i++) {
					sum2 += keyX[i];
				}

				Z = (sum1 + keyY[Ylength - 2]) % 10 * 10 + (sum2 + keyY[Ylength - 1]) % 10;

				/*
				 * 2.KeyGen step: minikey K = 1 + Z % 25
				 */
				int K = 1 + Z % 25;
				/*
				 * 3.Spiralize step: ciphertext=message Use the minikey K & to
				 * cipherText Z to decrypt the message O
				 */

				// initialize the matrix with the message
				StringBuilder sb = new StringBuilder();
				char matrix[][] = new char[layers][layers];
				int x[] = { 1, 0, -1 };
				int y[] = { 0, 1, -1 };
				int col = layers;
				int row = layers;
				int xStart = 0;
				int yStart = 0;
				int direction = 0;
				int addedCol = 0;
				int addedRow = 0;
				int candidateCells = 0;
				int addedCells = 0;
				for (int i = 0; i < message.length(); i++) {
					if (x[direction] == 0) {
						candidateCells = row - addedRow;
					} else {
						candidateCells = col - addedCol;
					}
					if (candidateCells < 0) {
						break;
					}
					matrix[xStart][yStart] = message.charAt(i);
					addedCells++;
					if (addedCells == candidateCells) {
						addedRow += 1;
						addedCol += 1;
						direction = (direction + 1) % 3;
						addedCells = 0;
					}
					xStart += x[direction];
					yStart += y[direction];
				}

				// Read message from matrix by original order and decryption
				for (int i = layers - 1; i >= 0; i--) {
					for (int j = i; j < layers; j++) {
						char c = matrix[j][i];
						if (c - K >= 65) {
							c = (char) (c - K);
						} else {
							c = (char) (90 - (K - (c - 64)));
						}
						sb.append(c);
					}
				}

				/* Write response */
				Date date = new Date();
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + df.format(date) + "\n" + sb.toString()
						+ "\n";
				response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
			}
		});
		
		/**
		 * Query 2
		 */
		router.route("/q2").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			HttpServerRequest request = routingContext.request();
			String hashtag = request.getParam("hashtag");
			String N = request.getParam("N");
			String keywordslist = request.getParam("list_of_key_words");
			String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
			
			// invalid parameter check
			if ("".equals(hashtag) || "".equals(keywordslist) || !N.matches(regex)) {
				response.setStatusCode(200).putHeader("content-type", "text/plain;charset=UTF-8").end(result);
			} else {
				/* get n and keywords list from request args */
				int n = Integer.valueOf(N);
				String[] keywords = keywordslist.split(",");
				PriorityQueue<KVPair> pq = new PriorityQueue<KVPair>(11, new Comparator<KVPair>() {
					@Override
					public int compare(KVPair o1, KVPair o2) {
						if (o1.getValue().equals(o2.getValue())) {
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
			        		JSONObject cacheKW = jo.getJSONObject("keywrods");
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
			        	ResultSet rs = stmt.executeQuery();
			        	JSONArray cacheJa = new JSONArray();
						while (rs.next()) {
							int score = 0;
							Long userid = Long.valueOf(rs.getString("user_id"));
							//get calculated keywords count from database
							JSONObject jo = new JSONObject(rs.getString("keywords"));
							JSONObject cacheObj = new JSONObject();
							cacheObj.put("user_id", userid);
							cacheObj.put("keywrods", jo);
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
					response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
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
		});
		
		
		
		
		
		
		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 80), "0.0.0.0",
				result -> {
					if (result.succeeded()) {
						fut.complete();
					} else {
						fut.fail(result.cause());
					}
				});
	}

}
