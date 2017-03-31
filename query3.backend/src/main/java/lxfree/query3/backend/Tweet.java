package lxfree.query3.backend;

public class Tweet {
	
	private String id;
	private String text;
	private int impact_socre;
	private int keyWordsFreq;
	
	public Tweet(String id, String text, int impact_socre, int keyWordsFreq) {
		this.id = id;
		this.text = text;
		this.impact_socre = impact_socre;
		this.keyWordsFreq = keyWordsFreq;
	}
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public int getImpact_socre() {
		return impact_socre;
	}
	public void setImpact_socre(int impact_socre) {
		this.impact_socre = impact_socre;
	}
	public int getKeyWordsFreq() {
		return keyWordsFreq;
	}
	public void setKeyWordsFreq(int keyWordsFreq) {
		this.keyWordsFreq = keyWordsFreq;
	}
	
	

}
