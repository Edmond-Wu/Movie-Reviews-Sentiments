package io.github.test;

public class Review {
	
	private String id;
	private String review;
	private String sentiment;
	
	public Review(String id, String review) {
		this.id = id;
		this.review = review;
		this.sentiment = "";
	}
	
	public String getID() {
		return id;
	}
	
	public String getReview() {
		return review;
	}
	
	public String getSentiment() {
		return sentiment;
	}
	
	public void setSentiment(String result) {
		sentiment = result;
	}
	
	/**
	 * Returns Review in json format
	 */
	public String toString() {
		return "{id: " + id + ", review: " + review + ", category: " + "\"" + sentiment + "\"}";
	}
}
