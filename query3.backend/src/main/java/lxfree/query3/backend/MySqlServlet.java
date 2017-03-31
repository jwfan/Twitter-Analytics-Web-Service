package lxfree.query3.backend;

import java.io.IOException;
import java.io.PrintWriter;
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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import lxfree.query2.backend.KVPair;

import org.json.JSONArray;

public class MySqlServlet extends HttpServlet {
	
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
        String startTime = request.getParameter("time_start");
        String endTime = request.getParameter("time_end");
        String startUid = request.getParameter("uid_start");
        String endUid = request.getParameter("uid_end");
        String maxTopicWords = request.getParameter("n1");
        String maxTweets = request.getParameter("n2");
        String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
        PrintWriter writer = response.getWriter();
        response.setStatus(200);
        response.setContentType("text/plain;charset=UTF-8");
        
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
        		Map<String, KeyWordTweets> wordsCount = new HashMap<String, KeyWordTweets>();
        		ResultSet rs = stmt.executeQuery(sql);
        		double totalNum = 0;
        		while(rs.next()) {
        			String tweet = new JSONObject(rs.getString("censored_text")).getString("censored_text");
        			int impactScore = rs.getInt("impact_score");
        			String tweetId = rs.getString("tweeter_id"); 
        			JSONObject keyWords = new JSONObject(rs.getString("keywords"));
        			for(String key: keyWords.keySet()){
        				if(wordsCount.containsKey(key)) {
        					wordsCount.get(key).addTweetsNum();;
        					wordsCount.get(key).addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
        				} else {
        					KeyWordTweets list = new KeyWordTweets(1);
        					list.addTweet(new Tweet(tweetId, tweet, impactScore, keyWords.getInt(key)));
        					wordsCount.put(key, list);
        				}
        			}
        			totalNum++;
        		}
            	PriorityQueue<KeyWordScore> pq = new PriorityQueue<KeyWordScore>(new Comparator<KeyWordScore>(){
            		@Override
            		public int compare(KeyWordScore o1, KeyWordScore o2) {
        	    		if(o1.getTopicScore() > o2.getTopicScore()) {
        	    			return -1;//generate the min heap for topic score
        	    		} else if(o1.getTopicScore() == o2.getTopicScore()){
        	    			return o1.getKey().compareTo(o2.getKey());
        	    		} else {
        	    			return 1;
        	    		}
            		}
            	});
        		for(Entry<String, KeyWordTweets> entry: wordsCount.entrySet()) {
        			double topicScore = 0.0;
        			KeyWordTweets kwt = entry.getValue();
        			int tweetsNum = kwt.getTweetsNum();
        			double idf = Math.log(totalNum/tweetsNum);
        			List<String> texts = new ArrayList<String>();
        			for(Tweet t: kwt.getTweets()) {
        				topicScore += t.getKeyWordsFreq()*idf*Math.log(t.getImpact_socre() + 1);
        				texts.add(t.getImpact_socre() + "\t" + t.getId() + "\t" +t.getText());
        			}
        			KeyWordScore s = new KeyWordScore(entry.getKey(), topicScore, texts);
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
        		StringBuilder sb = new StringBuilder();
        		StringBuilder tsb = new StringBuilder();
        		int tweetNum = 0;
        		while(pq.peek() != null) {
        			KeyWordScore kws = pq.poll();
        			sb.append(kws.getKey()).append(":").append(kws.getTopicScore()).append("\t");
        			for(String text : kws.getText()) {
        				if(tweetNum < Integer.valueOf(maxTweets)) {
        					tsb.append(text).append("\n");
        				}
        			}
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

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
