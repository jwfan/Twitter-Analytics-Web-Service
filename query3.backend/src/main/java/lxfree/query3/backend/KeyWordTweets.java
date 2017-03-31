package lxfree.query3.backend;

import java.util.ArrayList;
import java.util.List;

public class KeyWordTweets {
	
	private int tweetsNum;
	private List<Tweet> tweets;
	
	public KeyWordTweets(int n) {
		this.tweetsNum = n;
		this.tweets = new ArrayList<Tweet>();
	}

	public int getTweetsNum() {
		return tweetsNum;
	}

	public void addTweetsNum() {
		this.tweetsNum++;
	}

	public List<Tweet> getTweets() {
		return tweets;
	}

	public void addTweet(Tweet tweet) {
		this.tweets.add(tweet);
	}
}
