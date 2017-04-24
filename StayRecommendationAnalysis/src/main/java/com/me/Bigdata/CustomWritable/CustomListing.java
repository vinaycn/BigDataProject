package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomListing implements WritableComparable<CustomListing> {

	private String listingUrl;
	private double listingReviewsPerMonth;
	private String hostName;
	private String pictureUrl;
	private int id;
	
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(listingReviewsPerMonth);
		out.writeUTF(listingUrl);
		out.writeUTF(pictureUrl);
		out.writeUTF(hostName);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		listingReviewsPerMonth = in.readDouble();
		hostName = in.readUTF();
		pictureUrl = in.readUTF();
		listingUrl = in.readUTF();
	}

	@Override
	public int compareTo(CustomListing customListing) {
		if (customListing.listingReviewsPerMonth > this.listingReviewsPerMonth) {
			return 1;
		} else if (customListing.listingReviewsPerMonth < this.listingReviewsPerMonth) {
			return -1;
		} else {
			return 0;
		}
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return hostName + " Rating " + listingReviewsPerMonth;
	}

}
