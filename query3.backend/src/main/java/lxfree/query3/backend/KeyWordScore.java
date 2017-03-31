package lxfree.query3.backend;

import java.util.List;

public class KeyWordScore {

    private String key;
    private Double topicScore;
    private List<String> text;
    
	public KeyWordScore(String key, Double topicScore, List<String> text) {
		this.key = key;
		this.topicScore = topicScore;
		this.text = text;
	}

	public String getKey() {
		return key;
	}

	public Double getTopicScore() {
		return topicScore;
	}

	public List<String> getText() {
		return text;
	}
	
	

}
