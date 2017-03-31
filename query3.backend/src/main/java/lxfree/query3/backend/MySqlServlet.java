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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class MySqlServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static Connection conn;
	private static String TEAMID = "LXFreee";
	private static String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static String TABLENAME = "twitter";
	private final static String regex = "[0-9]+";
	private static Map<String, Integer> bannedWords = new HashMap<String, Integer>();

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
        
        Thread t = new Thread(new Runnable(){
			@Override
			public void run() {
				String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
		        //invalid parameter check
		        if(!startTime.matches(regex) || !endTime.matches(regex) || !startUid.matches(regex) 
		        		|| !endUid.matches(regex) || !maxTweets.matches(regex) || !maxTopicWords.matches(regex)) {
		        	writer.write(result);
		        	writer.close();
		        } else {
		        	PreparedStatement stmt = null;
		        	try {
		        		String sql = "SELECT twitter_id, censored_text, impact_score, keywords, word_count FROM " + TABLENAME + 
		        				" WHERE time_stamp between ? and ?" + 
		        				" and user_id between ? and ?";
		        		stmt = conn.prepareStatement(sql);
		        		stmt.setString(1, startTime);
		        		stmt.setString(2, endTime);
		        		stmt.setString(3, startUid);
		        		stmt.setString(4, endUid);
		        		Map<String, KeyWordTweets> wordsCount = new HashMap<String, KeyWordTweets>();//key word topic score
		        		Map<String, Integer> tweetsHash = new HashMap<String, Integer>();//remove replicated tweets
		        		ResultSet rs = stmt.executeQuery(sql);
		        		double totalNum = 0;
		        		while(rs.next()) {
		        			String tweet = new JSONObject(rs.getString("censored_text")).getString("censored_text");
		        			int impactScore = rs.getInt("impact_score");
		        			String tweetId = rs.getString("tweeter_id"); 
		        			JSONObject keyWords = new JSONObject(rs.getString("keywords"));
		        			for(String key: keyWords.keySet()){
		        				if(wordsCount.containsKey(key)) {
		        					wordsCount.get(key).addTweetsNum();
		        					wordsCount.get(key).addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
		        				} else {
		        					KeyWordTweets list = new KeyWordTweets(1);
		        					list.addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
		        					wordsCount.put(key, list);
		        				}
		        			}
		        			totalNum++;
		        		}
		        		//Store the top n1 topic words
		            	PriorityQueue<KeyWordScore> pq = new PriorityQueue<KeyWordScore>(new Comparator<KeyWordScore>(){
		            		@Override
		            		public int compare(KeyWordScore o1, KeyWordScore o2) {
		        	    		if(o1.getTopicScore() > o2.getTopicScore()) {
		        	    			return 1;//generate the min heap for topic score
		        	    		} else if(o1.getTopicScore() == o2.getTopicScore()){
		        	    			return o1.getKey().compareTo(o2.getKey());//generate the min heap for key
		        	    		} else {
		        	    			return -1;
		        	    		}
		            		}
		            	});
		            	//store top n2 tweets with topic words
		            	PriorityQueue<Tweet> tweetspq = new PriorityQueue<Tweet>(new Comparator<Tweet>(){
		            		@Override
		            		public int compare(Tweet o1, Tweet o2) {
		            			if(o1.getImpact_socre() > o2.getImpact_socre()) {
		            				return -1;
		            			} else if(o1.getImpact_socre() == o2.getImpact_socre()){
		            				if(Long.valueOf(o1.getId()) > Long.valueOf(o2.getId())) {
		            					return -1;
		            				} else if(o1.getId().equals(o2.getId())){
		            					return 0;
		            				} else {
		            					return 1;
		            				}
		            			} else {
		            				return 0;
		            			}
		            		}
		            	});
		            	//calculate the topic socre and put in the priority queue
		        		for(Entry<String, KeyWordTweets> entry: wordsCount.entrySet()) {
		        			double topicScore = 0.0;
		        			KeyWordTweets kwt = entry.getValue();
		        			int tweetsNum = kwt.getTweetsNum();
		        			double idf = Math.log(totalNum/tweetsNum);
		        			List<Tweet> tweetList = new ArrayList<Tweet>();
		        			for(Tweet t: kwt.getTweets()) {
		        				topicScore += t.getKeyWordsFreq()*idf*Math.log(t.getImpact_socre() + 1);
		        				tweetList.add(t);
		        			}
		        			KeyWordScore s = new KeyWordScore(entry.getKey(), topicScore, tweetList);
		        			if(pq.size() < Integer.valueOf(maxTopicWords)) {
		        				pq.add(s);
		        			} else {
		        				KeyWordScore peek = pq.peek();
		        				if(peek.getTopicScore() < s.getTopicScore()) {
		        					pq.poll();
		        					pq.add(s);
		        				} else if(peek.getTopicScore() == s.getTopicScore() 
		        						&& peek.getKey().compareTo(s.getKey())>0){
		        					pq.poll();
		        					pq.add(s);
		        				}
		        			}
		        		}
		        		//append topic words result
		        		StringBuilder sb = new StringBuilder();
		        		//append tweets result
		        		StringBuilder tsb = new StringBuilder();
		        		
		        		while(pq.peek() != null) {
		        			//output topic words:score
		        			KeyWordScore kws = pq.poll();
		        			String keyWord = kws.getKey();
		        			StringBuilder censorsb = new StringBuilder();
		        			if(bannedWords.containsKey(keyWord)) {
								// if banned words are found
								censorsb.append(keyWord.charAt(0));
								for(int i=1;i<keyWord.length()-1;i++) {
									censorsb.append("*");								
								}
								censorsb.append(keyWord.charAt(keyWord.length()-1));
		        			} else {
		        				censorsb.append(keyWord);
		        			}
		        			sb.append(censorsb).append(":").append(kws.getTopicScore()).append("\t");
		        			
		        			//add tweets into priority queue to get top n2 tweets
		        			List<Tweet> tList = kws.getTweetList();
		        			for(Tweet t: tList) {
		        				String tid = t.getId();
		        				if(!tweetsHash.containsKey(tid)) {
		        					tweetsHash.put(tid, 1);
		        					if(tweetspq.size() < Integer.valueOf(maxTweets)) {
		        						tweetspq.add(t);
		        					} else {
		        						Tweet peek = tweetspq.peek();
		        						if(peek.getImpact_socre() < t.getImpact_socre()) {
		        							tweetspq.poll();
		        							tweetspq.add(t);
		        						} else if(peek.getImpact_socre() == t.getImpact_socre() && 
		        								Long.valueOf(peek.getId()) < Long.valueOf(t.getId())) {
		        							tweetspq.poll();
		        							tweetspq.add(t);
		        						}
		        					}
		        				}
		        			}
		        		}
		        		//output impact score, tid, text
		        		while(tweetspq.peek() != null) {
		        			Tweet t = tweetspq.poll();
		        			tsb.append(t.getImpact_socre()).append("\t")
		        			.append(t.getId()).append("\t").append(t.getText()).append("\n");
		        		}
		        		result = sb.substring(0,sb.length()-1) + "\n" + tsb.substring(0,tsb.length() -1);
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
