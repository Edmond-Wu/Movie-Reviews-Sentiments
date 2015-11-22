package io.github.test;

import com.mongodb.*;
import java.util.*;
import java.io.*;

import io.github.sqlconnection.BaseConnection;

public class MongoConnection {
	
	/**
	 * Main method containing review analysis algorithm
	 */
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{		
		BaseConnection bc = new BaseConnection();
		bc.connect();
		
		ArrayList<String> pos_words = new ArrayList<String>();
		ArrayList<String> neg_words = new ArrayList<String>();
		
		fillWords("negative-words.txt", neg_words);
		fillWords("positive-words.txt", pos_words);
		
		//Cursors for the 2 different json files
		bc.setDBAndCollection("cs336", "unlabel_review_after_splitting");
		DBCursor split = bc.showRecords();
		bc.setDBAndCollection("cs336", "unlabel_review");
		DBCursor no_split = bc.showRecords();		
		
		//Reviews arraylist
		ArrayList<Review> reviews = new ArrayList<Review>();
		
		int positives = 0, negatives = 0;
		
		//Iterate through each entry in split list
		while(split.hasNext()){
			int sentiment_score = 0;
			DBObject split_dbo = split.next();
			DBObject no_split_dbo = no_split.next();
			Review review = new Review((String) no_split_dbo.get("id"), (String) no_split_dbo.get("review"));
			BasicDBList word_count_list = (BasicDBList) split_dbo.get("review");			
			
			//Sentiments algorithm
			for(Object o : word_count_list){
				DBObject instance = (DBObject) o;
				int count = (int) instance.get("count");
				String word = (String) instance.get("word");
				
				if(neg_words.contains(word)){
					sentiment_score -= count;
				}
				else if(pos_words.contains(word)){
					sentiment_score += count;
				}
			}
			if (sentiment_score < 0) {
				review.setSentiment("negative");
				negatives++;
			}
			else {
				review.setSentiment("positive");
				positives++;
			}
			reviews.add(review); //add review to reviews list
		}
		writeFile(reviews); //write to json file
		System.out.println("Number of positive reviews: " + positives);
		System.out.println("Number of negative reviews: " + negatives);
		bc.close();
	}
	
	/**
	 * Method to fill the positives/negatives arraylist
	 */
	public static void fillWords(String file_name, ArrayList<String> words) {
		File file = new File(file_name);
		try{
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(file);
			String word;
			while (scan.hasNextLine()) {
				word = scan.next();
				words.add(word);
			}
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to create the json file containing categorized reviews
	 */
	public static void writeFile(ArrayList<Review> reviews) throws FileNotFoundException, UnsupportedEncodingException {
		Writer writer = null;
		try {
		    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("reviews.json"), "utf-8"));
		    for(Review review : reviews){
				writer.write(review.toString() + "\n");
			}
		}
		catch (IOException ex) {}
		finally {
		   try {
			   writer.close();
		   } 
		   catch (Exception ex) {}
		}
	}
}
