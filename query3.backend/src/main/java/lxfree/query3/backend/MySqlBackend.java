package lxfree.query3.backend;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class MySqlBackend extends AbstractVerticle {
	private static Connection conn1;
	private static Connection conn2;
	// private static final long serialVersionUID = 1L;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "q3_table";
	private final static String regex = "[0-9]+";
	private static Map<String, Integer> bannedWords = new HashMap<String, Integer>();
	private static ConcurrentHashMap<String, KeyWordTweets> wordsCount = new ConcurrentHashMap<String, KeyWordTweets>();
	private static Double totalNum = 0.0;
	private static int choose = 0;

	public MySqlBackend() {
		try {
			conn1 = ConnectionManager.getMySqlConnections(0);
			// System.out.println("Connect to database 1 done.");
			conn2 = ConnectionManager.getMySqlConnections(1);
			// System.out.println("Connect to database 2 done.");
			// conn3 = ConnectionManager.getConnection(2);
			// System.out.println("Connect to database 3 done.");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * query a single sub-database
	 */
	public void qrySingleDB(String startTime, String endTime, String startUid, String endUid) {
		PreparedStatement stmt = null;
		try {
			String sql = "SELECT twitter_id, censored_text, impact_score, keywords FROM " + TABLENAME
					+ " WHERE time_stamp>=? AND time_stamp<=? and user_id>=? and user_id<=?";
			/* Decide to which database to query */
			switch (choose) {
			case 0:
				stmt = conn1.prepareStatement(sql);
				break;
			case 1:
				stmt = conn2.prepareStatement(sql);
				break;
			// case 2:stmt = conn3.prepareStatement(sql);break;
			default:
				if (choose % 2 == 0)
					stmt = conn1.prepareStatement(sql);
				else
					stmt = conn2.prepareStatement(sql);
			}
			// System.out.println("Choose database " + choose);
			// choose=(choose+1)%3;
			choose = (choose + 1) % 2;
			stmt.setString(1, startTime);
			stmt.setString(2, endTime);
			stmt.setString(3, startUid);
			stmt.setString(4, endUid);
			// remove replicated tweets
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String tweet = null;
				JSONObject text = new JSONObject(rs.getString("text"));
				tweet = text.getString("censored_text");
				int impactScore = rs.getInt("impact_score");
				String tweetId = rs.getString("twitter_id");
				JSONObject keyWords = new JSONObject(rs.getString("keywords"));
				for (String key : keyWords.keySet()) {
					if (wordsCount.containsKey(key)) {
						wordsCount.get(key).addTweetsNum();
						wordsCount.get(key).addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
					} else {
						KeyWordTweets list = new KeyWordTweets(1);
						list.addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
						wordsCount.putIfAbsent(key, list);
					}
				}
				synchronized (totalNum) {
					totalNum++;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void start(Future<Void> fut) {
		Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));
		Router router = Router.router(vertx);
		HttpServerOptions options = new HttpServerOptions();
		router.route("/q3").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			String startTime = routingContext.request().getParam("time_start");
			String endTime = routingContext.request().getParam("time_end");
			String startUid = routingContext.request().getParam("uid_start");
			String endUid = routingContext.request().getParam("uid_end");
			String maxTopicWords = routingContext.request().getParam("n1");
			String maxTweets = routingContext.request().getParam("n2");

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
			if (!startTime.matches(regex) || !endTime.matches(regex) || !startUid.matches(regex)
					|| !endUid.matches(regex) || !maxTweets.matches(regex) || !maxTopicWords.matches(regex)) {
				response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
			} else {
				wordsCount = new ConcurrentHashMap<String, KeyWordTweets>();
				Map<String, Integer> tweetsHash = new HashMap<String, Integer>();
				totalNum = 0.0;
				qrySingleDB(startTime, endTime, startUid, endUid);
				// Store the top n1 topic words
				PriorityQueue<KeyWordScore> pq = new PriorityQueue<KeyWordScore>(11, new Comparator<KeyWordScore>() {
					@Override
					public int compare(KeyWordScore o1, KeyWordScore o2) {
						if (o1.getTopicScore() > o2.getTopicScore()) {
							return 1;// generate the min heap for topic score
						} else if (o1.getTopicScore() == o2.getTopicScore()) {
							return o1.getKey().compareTo(o2.getKey());
						} else {
							return -1;
						}
					}
				});
				// store top n2 tweets with topic words
				PriorityQueue<Tweet> tweetspq = new PriorityQueue<Tweet>(11, new Comparator<Tweet>() {
					@Override
					public int compare(Tweet o1, Tweet o2) {
						if (o1.getImpact_socre() > o2.getImpact_socre()) {
							return -1;
						} else if (o1.getImpact_socre() == o2.getImpact_socre()) {
							if (Long.valueOf(o1.getId()) > Long.valueOf(o2.getId())) {
								return -1;
							} else if (o1.getId().equals(o2.getId())) {
								return 0;
							} else {
								return 1;
							}
						} else {
							return 0;
						}
					}
				});
				for (Entry<String, KeyWordTweets> entry : wordsCount.entrySet()) {
					double topicScore = 0.0;
					KeyWordTweets kwt = entry.getValue();
					int tweetsNum = kwt.getTweetsNum();
					double idf = Math.log(totalNum / tweetsNum);
					List<Tweet> tweetList = new ArrayList<Tweet>();
					for (Tweet t : kwt.getTweets()) {
						topicScore += t.getKeyWordsFreq() * idf * Math.log(t.getImpact_socre() + 1);
						tweetList.add(t);
					}
					KeyWordScore s = new KeyWordScore(entry.getKey(), topicScore, tweetList);
					if (pq.size() < Integer.valueOf(maxTopicWords)) {
						pq.add(s);
					} else {
						KeyWordScore peek = pq.peek();
						if (peek.getTopicScore() < s.getTopicScore()) {
							pq.poll();
							pq.add(s);
						} else if (peek.getTopicScore() == s.getTopicScore()
								&& peek.getKey().compareTo(s.getKey()) > 0) {
							pq.poll();
							pq.add(s);
						}
					}
				}
				// append topic words result
				StringBuilder sb = new StringBuilder();
				// append tweets result
				StringBuilder tsb = new StringBuilder();
				while (pq.peek() != null) {
					// output topic words:score
					KeyWordScore kws = pq.poll();
					String keyWord = kws.getKey();
					StringBuilder censorsb = new StringBuilder();
					if (bannedWords.containsKey(keyWord)) {
						// if banned words are found
						censorsb.append(keyWord.charAt(0));
						for (int i = 1; i < keyWord.length() - 1; i++) {
							censorsb.append("*");
						}
						censorsb.append(keyWord.charAt(keyWord.length() - 1));
					} else {
						censorsb.append(keyWord);
					}
					String s = censorsb + ":" + kws.getTopicScore() + "\t";
					sb.insert(0, s);

					// add tweets into priority queue to get top n2 tweets
					List<Tweet> tList = kws.getTweetList();
					for (Tweet t : tList) {
						String tid = t.getId();
						if (!tweetsHash.containsKey(tid)) {
							tweetsHash.put(tid, 1);
							if (tweetspq.size() < Integer.valueOf(maxTweets)) {
								tweetspq.add(t);
							} else {
								Tweet peek = tweetspq.peek();
								if (peek.getImpact_socre() < t.getImpact_socre()) {
									tweetspq.poll();
									tweetspq.add(t);
								} else if (peek.getImpact_socre() == t.getImpact_socre()
										&& Long.valueOf(peek.getId()) < Long.valueOf(t.getId())) {
									tweetspq.poll();
									tweetspq.add(t);
								}
							}
						}
					}
				}
				while (tweetspq.peek() != null) {
					Tweet t = tweetspq.poll();
					StringBuilder tweetSb = new StringBuilder();
					tweetSb.append(t.getImpact_socre()).append("\t").append(t.getId()).append("\t").append(t.getText())
							.append("\n");
					tsb.insert(0, tweetSb);
				}
				result = sb.substring(0, sb.length() - 1) + "\n" + tsb.substring(0, tsb.length() - 1);
				response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
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
