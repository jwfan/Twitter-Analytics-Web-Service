package lxfree.query3.backend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class MySqlServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "q3_table";
	private final static String regex = "[0-9]+";
	private static Map<String, Integer> bannedWords = new HashMap<String, Integer>();
	private static String shardBase1="00013999096180000000000024283520";
	private static String shardBase2="00014825227340000000000227019820";
	private static Integer roundRobin1=0;
	private static Integer roundRobin2=0;
	private static Integer roundRobin3=0;
	/* Whether each part of the database need to be queried */
	private static boolean q1Flag=false;
	private static boolean q2Flag=false;
	private static boolean q3Flag=false;
	private static ConcurrentHashMap<String, KeyWordTweets> wordsCount = new ConcurrentHashMap<String, KeyWordTweets>();
	private static Double totalNum=0.0;
	/* Whether each part of the database has been queried */
	private static boolean q1Done=false;
	private static boolean q2Done=false;
	private static boolean q3Done=false;
	private static Object lock;
	
	/**
	 * Check whether each part of the database needed to be queried
	 * and set the corresponding flag
	 */
	public void setQryFlag(String startTUid, String endTUid){
		/* the 1st part of the database */
		if(startTUid.compareTo(shardBase1)<0){
			q1Flag=true;
			/* the 2nd part of the database */
			if(endTUid.compareTo(shardBase1)<0){
				q2Flag=false;
				/* the 3rd part of the database */
				q3Flag=false;
			}else{
				q2Flag=true;
				/* the 3rd part of the database */
				if(endTUid.compareTo(shardBase2)<0){
					q3Flag=false;
				}else{
					q3Flag=true;
				}
			}
		}else{
			q1Flag=false;
			/* the 2nd part of the database */
			if(startTUid.compareTo(shardBase2)<0){
				q2Flag=true;
				/* the 3rd part of the database */
				if(endTUid.compareTo(shardBase2)<0){
					q3Flag=false;
				}else{
					q3Flag=true;
				}
			}else{
				q2Flag=false;
				/* the 3rd part of the database */
				q3Flag=true;
			}
		}
	}
	
	/**
	 * Query the 3 sub-databases according to query flags
	 */
	public void qryDB(final String startTUid, final String endTUid){
		q1Done=false;
		q2Done=false;
		q3Done=false;
		/* Query the 1st sub-database */
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				if(q1Flag==false){
					/* No need to query sub-database 1 */
					synchronized(lock){
						q1Done=true;
						lock.notifyAll();
					}
				}else{
					/* Check the query range */
					if(q2Flag==true){
						qrySingleDB(startTUid, shardBase1, roundRobin1);
						roundRobin1=(roundRobin1+1)%2;
					}else{
						qrySingleDB(startTUid, endTUid, roundRobin1);
						roundRobin1=(roundRobin1+1)%2;
					}
				}
				System.out.println("sub-db 1 query done.");
			}
		});
		
		/* Query the 2nd sub-database */
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				if(q2Flag==false){
					/* No need to query sub-database 1 */
					synchronized(lock){
						q2Done=true;
						lock.notifyAll();
					}
				}else{
					/* Check the query range */
					if(q1Flag&&q3Flag){
						qrySingleDB(startTUid, endTUid, roundRobin2);
						roundRobin2=(roundRobin2+1)%2+2;
					}else if(q1Flag&&!q3Flag){
						qrySingleDB(shardBase1, endTUid, roundRobin2);
						roundRobin2=(roundRobin2+1)%2+2;
					}else if(!q1Flag&&q3Flag){
						qrySingleDB(startTUid, shardBase2, roundRobin2);
						roundRobin2=(roundRobin2+1)%2+2;
					}else{
						qrySingleDB(startTUid, endTUid, roundRobin2);
						roundRobin2=(roundRobin2+1)%2+2;
					}
				}
				System.out.println("sub-db 2 query done.");
			}
		});
		
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				if(q3Flag==false){
					/* No need to query sub-database 1 */
					synchronized(lock){
						q3Done=true;
						lock.notifyAll();
					}
				}else{
					/* Check the query range */
					if(q2Flag){
						qrySingleDB(shardBase2, endTUid, roundRobin3);
						roundRobin3=(roundRobin3+1)%2+4;
					}else{
						qrySingleDB(startTUid, endTUid, roundRobin2);
						roundRobin3=(roundRobin3+1)%2+4;
					}
				}
				System.out.println("sub-db 3 query done.");
			}
		});
		t1.start();
		t2.start();
		t3.start();
	}
	
	/**
	 * query a single sub-database
	 */
	public void qrySingleDB(String startTUid, String endTUid, int choose){
		Connection conn=null;
		PreparedStatement stmt = null;
		try {
			conn = ConnectionManager.getConnection(choose);
			String sql = "SELECT tu_id, censored_text, impact_score, keywords FROM " + TABLENAME
					 	 + " WHERE tu_id BETWEEN ? AND ?";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, startTUid);
			stmt.setString(2, endTUid);
			// remove replicated tweets
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				String tweet = null;
				String text = rs.getString("text");
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
				synchronized(totalNum){
					totalNum++;
				}
			}
		} catch (ClassNotFoundException | SQLException e) {
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
		response.setStatus(200);
		response.setContentType("text/plain;charset=UTF-8");
		final PrintWriter writer = response.getWriter();
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
		// invalid parameter check
		if (!startTime.matches(regex) || !endTime.matches(regex) || !startUid.matches(regex) || !endUid.matches(regex)
				|| !maxTweets.matches(regex) || !maxTopicWords.matches(regex)) {
			writer.write(result);
			writer.close();
		} else {
			String zero13 = "0000000000000";
			String zero19 = "0000000000000000000";
			String timestamp13 = zero13.substring(0, 13 - startTime.length()) + startTime;
			String uid19 = zero19.substring(0, 19 - startUid.length()) + startUid;
			final String startTUid = timestamp13+uid19;
			timestamp13 = zero13.substring(0, 13 - endTime.length()) + endTime;
			uid19 = zero19.substring(0, 19 - endUid.length()) + endUid;
			final String endTUid = timestamp13+uid19;
			wordsCount = new ConcurrentHashMap<String, KeyWordTweets>();
			Map<String, Integer> tweetsHash = new HashMap<String, Integer>();
			totalNum = 0.0;
			/* 
			 * set query flag 
			 */
			setQryFlag(startTUid, endTUid);
			try {
				/*
				 * Query the 3 sub-databases
				 */
				qryDB(startTUid, endTUid);				
				//Store the top n1 topic words
				PriorityQueue<KeyWordScore> pq = new PriorityQueue<KeyWordScore>(11, new Comparator<KeyWordScore>() {
					@Override
					public int compare(KeyWordScore o1, KeyWordScore o2) {
						if (o1.getTopicScore() > o2.getTopicScore()) {
							return 1;// generate the min heap for topic score
						} else if (o1.getTopicScore() == o2.getTopicScore()) {
							return o1.getKey().compareTo(o2.getKey());// generate the min heap for key
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
				
				/*
				 * Wait for the query operations to synchronize
				 */
				synchronized(lock){
					while(q1Done==false || q2Done==false || q3Done==false){
						System.out.println("Waiting for query to finish...");
						lock.wait();
					}
					// calculate the topic socre and put in the priority queue
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
				// output impact score, tid, text
				while (tweetspq.peek() != null) {
					Tweet t = tweetspq.poll();
					StringBuilder tweetSb = new StringBuilder();
					tweetSb.append(t.getImpact_socre()).append("\t").append(t.getId()).append("\t").append(t.getText())
							.append("\n");
					tsb.insert(0, tweetSb);
				}
				result = sb.substring(0, sb.length() - 1) + "\n" + tsb.substring(0, tsb.length() - 1);
				writer.write(result);
				writer.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
