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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

public class VertexServer extends AbstractVerticle {

	private final static String TEAMID = "LXFreee";
	private final static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private String X = "12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140";
	private static Connection conn1;
	private static Connection conn2;
	private static int choose = 0;
	private static String TABLENAME_Q2 = "q2_table";
	private static String TABLENAME_Q4 = "q4_table";
	private final static String regex = "[0-9]+";
	private static Map<String, JsonArray> cache = new HashMap<String, JsonArray>();
	private static String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
	ConcurrentHashMap<String, PriorityBlockingQueue<RequestWrapping>> readMap = new ConcurrentHashMap<String, PriorityBlockingQueue<RequestWrapping>>();
	final ConcurrentHashMap<String, PriorityBlockingQueue<RequestWrapping>> opMap = new ConcurrentHashMap<String, PriorityBlockingQueue<RequestWrapping>>();
	final Map<String, Integer> seqMap = new HashMap<String, Integer>();

	@Override
	public void start(Future<Void> fut) {
		Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));
		Router router = Router.router(vertx);
		try {
			conn1 = ConnectionManager.getConnection(0);
			// System.out.println("Connect to database 1 done.");
			conn2 = ConnectionManager.getConnection(1);
			// System.out.println("Connect to database 2 done.");
			// conn3 = ConnectionManager.getConnection(2);
			// System.out.println("Connect to database 3 done.");
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
				response.setStatusCode(200).putHeader("content-type", "text/plain;charset=UTF-8")
						.end(String.format("INVALID"));
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
				String res = result + df.format(date) + "\n" + sb.toString() + "\n";
				response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(res);
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
					if (cache.containsKey(hashtag)) {
						JsonArray cacheRes = cache.get(hashtag);
						for (int i = 0; i < cacheRes.size(); i++) {
							JsonObject jo = cacheRes.getJsonObject(i);
							Long userid = jo.getLong("user_id");
							JsonObject cacheKW = jo.getJsonObject("keywrods");
							int score = 0;
							for (int j = 0; j < keywords.length; j++) {
								try {
									score += cacheKW.getInteger(keywords[j]);
								} catch (Exception e) {
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
						String sql = "SELECT hashtag, user_id, keywords FROM " + TABLENAME_Q2 + " where hashtag=?";
						/* Decide to which database to query */
						switch (choose) {
						case 0:
							stmt = conn1.prepareStatement(sql);
							break;
						case 1:
							stmt = conn2.prepareStatement(sql);
							break;
						default:
							if (choose % 2 == 0)
								stmt = conn1.prepareStatement(sql);
							else
								stmt = conn2.prepareStatement(sql);
						}
						choose = (choose + 1) % 2;
						stmt.setString(1, hashtag);
						ResultSet rs = stmt.executeQuery();
						JsonArray cacheJa = new JsonArray();
						while (rs.next()) {
							int score = 0;
							Long userid = Long.valueOf(rs.getString("user_id"));
							// get calculated keywords count from database
							JsonObject jo = new JsonObject(rs.getString("keywords"));
							JsonObject cacheObj = new JsonObject();
							cacheObj.put("user_id", userid);
							cacheObj.put("keywrods", jo);
							cacheJa.add(cacheObj);
							for (int i = 0; i < keywords.length; i++) {
								try {
									score += jo.getInteger(keywords[i]);
								} catch (Exception e) {
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
					}
					String resp = result;
					if (pq.size() > 0) {
						StringBuilder res = new StringBuilder();
						while (pq.peek() != null) {
							KVPair peek = pq.poll();
							String s = peek.getKey() + ":" + peek.getValue() + ",";
							res.insert(0, s);
						}
						resp = resp + res.substring(0, res.length() - 1) + "\n";
					}
					response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(resp);
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

//		router.route("/q4").handler(routingContext -> {
//			HttpServerResponse response = routingContext.response();
//			HttpServerRequest request = routingContext.request();
//			String operation = request.getParam("op");
//			String field = request.getParam("field");
//			String tid1 = request.getParam("tid1");
//			String tid2 = request.getParam("tid2");
//			String payload = request.getParam("payload");
//			String uuid = request.getParam("uuid");
//			Integer seq = Integer.valueOf(request.getParam("seq"));
//			// initialize sequence 1 for uuid
//			if (!seqMap.containsKey(uuid) || seq.equals(1)) {
//				seqMap.put(uuid, 1);
//			}
//
//			// READ
//			if ("read".equals(operation)) {
//				// invalid parameter check
//				Thread t = new Thread(new Runnable() {
//					public void run() {
//						// synchronized (opMap.get(uuid)) {
//						while (!seq.equals(seqMap.get(uuid))) {
//							try {
//								Thread.sleep(0);
//							} catch (InterruptedException e) {
//								e.printStackTrace();
//							}
//						}
//						PreparedStatement stmt = null;
//						try {
//							String sql = "SELECT twitter_id, timestamp, tuid, tusername,ttext,tfavorite_count,tretweet_count FROM "
//									+ TABLENAME_Q4 + " WHERE and twitter_id>=? and twitter_id<=?";
//							stmt = conn1.prepareStatement(sql);
//							stmt.setString(1, tid1);
//							stmt.setString(2, tid2);
//							// remove replicated tweets
//							ResultSet rs = stmt.executeQuery();
//							String res = result;
//							while (rs.next()) {
//								String tweetId = rs.getString("twitter_id");
//								String tuid = rs.getString("tuid");
//								String tusername = rs.getString("tusername");
//								String ttext = rs.getString("ttext");
//								Long timestamp = rs.getLong("timestamp");
//								int tfavoriteCount = rs.getInt("tfavorite_count");
//								int tretweetCount = rs.getInt("tretweet_count");
//								res += tweetId + "\\" + timestamp + "\\" + tuid + "\\" + tusername + "\\" + ttext + "\\"
//										+ tfavoriteCount + "\\" + tretweetCount + "\n";
//							}
//							response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(res);
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//						seqMap.put(uuid, seqMap.get(uuid) + 1);
//
//						if (!readMap.containsKey(uuid)) {
//							readMap.putIfAbsent(uuid, new PriorityBlockingQueue<RequestWrapping>());
//						}
//						RequestWrapping req = new RequestWrapping(operation, field, tid1, tid2, payload, uuid, seq);
//						readMap.get(uuid).add(req);
//
//						synchronized (readMap.get(uuid)) {
//							while (opMap.get(uuid).size() > 0 && readMap.get(uuid).size() > 0) {
//								RequestWrapping putPeek = putQueue.peek();
//								if (putPeek == null || putPeek.getTimestamp() > requestWrap.getTimestamp()) {
//									break;
//								} else {
//									try {
//										(putMap.get(key)).wait();
//									} catch (InterruptedException e) {
//										e.printStackTrace();
//									}
//								}
//							}
//						}
//						while (opMap.containsKey(uuid) && opMap.get(uuid).peek() < seq && seqMap.containsKey(uuid)
//								&& seqMap.get(uuid) < seq) {
//							try {
//								Thread.sleep(0);
//							} catch (InterruptedException e) {
//								e.printStackTrace();
//							}
//						}
//						// querytfavorite_count,
//						PreparedStatement stmt = null;
//						try {
//							String sql = "SELECT twitter_id, timestamp, tuid, tusername,ttext,tfavorite_count,tretweet_count FROM "
//									+ TABLENAME_Q4 + " WHERE and twitter_id>=? and twitter_id<=?";
//							stmt = conn1.prepareStatement(sql);
//							stmt.setString(1, tid1);
//							stmt.setString(2, tid2);
//							// remove replicated tweets
//							ResultSet rs = stmt.executeQuery();
//							String res = result;
//							while (rs.next()) {
//								String tweetId = rs.getString("twitter_id");
//								String tuid = rs.getString("tuid");
//								String tusername = rs.getString("tusername");
//								String ttext = rs.getString("ttext");
//								Long timestamp = rs.getLong("timestamp");
//								int tfavoriteCount = rs.getInt("tfavorite_count");
//								int tretweetCount = rs.getInt("tretweet_count");
//								res = res + tweetId + "\\" + timestamp + "\\" + tuid + "\\" + tusername + "\\" + ttext
//										+ "\\" + tfavoriteCount + "\\" + tretweetCount + "\n";
//							}
//							response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(res);
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//						// opwaitMap.get(uuid).poll();
//						opMap.get(uuid).notifyAll();
//						// }
//					}
//				});
//				t.start();
//			}
//
//			response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
//		});

		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 80), "0.0.0.0",
				result -> {
					if (result.succeeded()) {
						fut.complete();
					} else {
						fut.fail(result.cause());
					}
				});
	}

	/**
	 * Wrap the request with op, field, tid1, tid2, payload, uuid and sequence
	 */
	private class RequestWrapping implements Comparable<RequestWrapping> {
		private String operation;
		private String field;
		private String tid1;
		private String tid2;
		private String payload;
		private String uuid;
		private Integer sequence;

		private RequestWrapping(String operation, String field, String tid1, String tid2, String payload, String uuid,
				Integer sequence) {
			this.operation = operation;
			this.field = field;
			this.tid1 = tid1;
			this.tid2 = tid2;
			this.payload = payload;
			this.uuid = uuid;
			this.sequence = sequence;
		}

		private String getOperation() {
			return operation;
		}

		private String getField() {
			return field;
		}

		private String getTid1() {
			return tid1;
		}

		private String getTid2() {
			return tid2;
		}

		private String getPayload() {
			return payload;
		}

		private String getUuid() {
			return uuid;
		}

		private Integer getSequence() {
			return sequence;
		}

		@Override
		public int compareTo(RequestWrapping o) {
			if (this.getSequence() > o.getSequence()) {
				return 1;
			} else if (this.getSequence() < o.getSequence()) {
				return -1;
			} else {
				return 0;
			}
		}

	}

}
