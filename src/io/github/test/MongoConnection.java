package io.github.test;

import com.mongodb.*;
import java.util.*;
import java.io.*;

import io.github.sqlconnection.BaseConnection;

public class MongoConnection {
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{		
		BaseConnection bc = new BaseConnection();
		bc.connect();
		bc.showDBs();
		
		//Type of value doesn't matter; only key does
		HashMap<String, String> positives = new HashMap<String, String>();
		HashMap<String, String> negatives = new HashMap<String, String>();
		
		fillHashmap("negative-words.txt", negatives);
		fillHashmap("positive-words.txt", positives);
		
		//Cursors for the 2 different json files
		bc.setDBAndCollection("cs336", "unlabel_review_after_splitting");
		DBCursor split = bc.showRecords();
		bc.setDBAndCollection("cs336", "unlabel_review");
		DBCursor no_split = bc.showRecords();		
		
		//Reviews hashmap
		HashMap<Review, String> reviews = new HashMap<Review, String>();
		
		//Iterate through each entry in split list
		while(split.hasNext()){
			DBObject split_dbo = split.next();
			DBObject no_split_dbo = no_split.next();
			Review review = new Review((String) no_split_dbo.get("id"), (String) no_split_dbo.get("review"));
			int sentiment_score = 0;
			BasicDBList word_count_list = (BasicDBList) split_dbo.get("review");
			
			//Sentiments algorithm
			for(Object o : word_count_list){
				DBObject instance = (DBObject) o;
				int count = (int) instance.get("count");
				String word = (String) instance.get("word");
				
				if(positives.containsKey(word)){
					sentiment_score += count;
				}
				else if(negatives.containsKey(word)){
					sentiment_score -= count;
				}
			}
			if (sentiment_score > 0) {
				review.setSentiment("positive");
			}
			else {
				review.setSentiment("negative");
			}
			reviews.put(review, review.getSentiment());
		}
		
		writeFile(reviews);
		bc.close();
	}
	
	/**
	 * Method to fill the positives/negatives hashmap
	 */
	public static void fillHashmap(String file_name, HashMap<String, String> map) {
		File file = new File(file_name);
		try{
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(file);
			String word;
			while (scan.hasNextLine()) {
				word = scan.next();
				map.put(word, word);
			}
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to create the json file containing categorized reviews
	 */
	public static void writeFile(HashMap<Review, String> map) throws FileNotFoundException, UnsupportedEncodingException {
		Writer writer = null;
		try {
		    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("reviews.json"), "utf-8"));
		    for(Review review : map.keySet()){
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
