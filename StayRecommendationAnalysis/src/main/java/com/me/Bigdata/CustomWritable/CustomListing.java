package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CustomListing implements Writable {

	private String listingUrl;
	private double listingReviewsPerMonth;
	private String hostName;
	private String pictureUrl;
	private int id;
	private String price;
	
	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public CustomListing() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readInt();
		hostName =in.readUTF();
		pictureUrl =in.readUTF();
		listingUrl =in.readUTF();
		listingReviewsPerMonth = in.readDouble();
		price = in.readUTF();
		
		
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(listingUrl);

		out.writeUTF(hostName);
		out.writeUTF(pictureUrl);
		out.writeDouble(listingReviewsPerMonth);
		out.writeUTF(price);
	
		
	}

	public String getListingUrl() {
		return listingUrl;
	}

	public void setListingUrl(String listingUrl) {
		this.listingUrl = listingUrl;
	}

	public double getListingReviewsPerMonth() {
		return listingReviewsPerMonth;
	}

	public void setListingReviewsPerMonth(double listingReviewsPerMonth) {
		this.listingReviewsPerMonth = listingReviewsPerMonth;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getPictureUrl() {
		return pictureUrl;
	}

	public void setPictureUrl(String pictureUrl) {
		this.pictureUrl = pictureUrl;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	

}
