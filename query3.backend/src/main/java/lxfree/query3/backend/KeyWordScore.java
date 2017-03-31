package lxfree.query3.backend;

import java.util.List;

public class KeyWordScore {

    private String key;
    private Double topicScore;
    private List<Tweet> tweet;
    
	public KeyWordScore(String key, Double topicScore, List<Tweet> tweet) {
		this.key = key;
		this.topicScore = topicScore;
		this.tweet = tweet;
	}

	public String getKey() {
		return key;
	}

	public Double getTopicScore() {
		return topicScore;
	}

	public List<Tweet> getTweetList() {
		return tweet;
	}

}
