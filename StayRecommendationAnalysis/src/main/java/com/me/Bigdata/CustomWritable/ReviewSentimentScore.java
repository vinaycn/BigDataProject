package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ReviewSentimentScore implements Writable{

	
	private String review;
	private int sentimentScore;
	
	public String getReview() {
		return review;
	}

	public void setReview(String review) {
		this.review = review;
	}

	public int getSentimentScore() {
		return sentimentScore;
	}

	public void setSentimentScore(int sentimentScore) {
		this.sentimentScore = sentimentScore;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		sentimentScore =arg0.readInt();
		review = arg0.readLine();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(sentimentScore);
		arg0.writeUTF(review);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return review + "Score is " + sentimentScore;
	}
}
