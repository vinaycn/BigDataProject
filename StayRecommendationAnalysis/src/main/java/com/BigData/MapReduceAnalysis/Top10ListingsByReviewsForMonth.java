package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.BigData.utils.ColumnParser;
import com.me.Bigdata.CustomWritable.CustomListing;

public class Top10ListingsByReviewsForMonth {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top10ListingsByReviewsForMonth");
		conf.set("Place", "Berlin");
		job.setJarByClass(Top10ListingsByReviewsForMonth.class);
		job.setMapperClass(Top10ListingsByReviewsPerMonthMapper.class);

		job.setMapOutputKeyClass(CustomListing.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(CustomListing.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(Top10ListingsByReviewsForMonthReducer.class);
		job.addCacheFile(new URI("/Stay/Cache/headerForBerlin#headerForBerlin"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
	
	private static class Top10ListingsByReviewsPerMonthMapper extends Mapper<Object, Text,CustomListing,NullWritable>{
		
		
		
		private TreeMap<CustomListing,String> listingsMap;
		String[] headerList;
		int indexOfReviesPerMonth;
		int indexOfListingId;
		int indexOfListingUrl;
		int indexOfHostName;
		int indexOfPictureUrl;
		IntWritable Outkey = new IntWritable();
		
		
		
		@Override
		protected void setup(Mapper<Object, Text,CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			try{
				listingsMap = new TreeMap<CustomListing,String>();
				BufferedReader bufferedReader = new BufferedReader(new FileReader("headerForBerlin"));
		        headerList= bufferedReader.readLine().split("\t");
		        indexOfReviesPerMonth =ColumnParser.getTheIndexOfTheColumn(headerList,"reviews_per_month");
		        indexOfListingId =  ColumnParser.getTheIndexOfTheColumn(headerList,"id");
		        indexOfPictureUrl = ColumnParser.getTheIndexOfTheColumn(headerList,"picture_url");
		        indexOfHostName =  ColumnParser.getTheIndexOfTheColumn(headerList,"host_name");
		        indexOfListingUrl = ColumnParser.getTheIndexOfTheColumn(headerList,"listing_url");
		        
		        System.out.println("indexofListingId" + indexOfListingId);
		        System.out.println("Review" + indexOfReviesPerMonth);
				}catch (Exception e) {
					System.out.println("Something Went Wrong");
				}
			
			
			
		}
		
		static {
			File f = new File("/home/vinay/Desktop/mapoutput");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text,CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			String val[] = value.toString().split("\t");
			if (value.toString().contains("price") || val.length != 95)
				return;
			
			System.out.println("length of the line" +val.length);
			int id = Integer.valueOf(val[indexOfListingId]);
			String listingUrl = val[indexOfListingUrl];
			String pictureUrl =val[indexOfPictureUrl];
			String hostName = val[indexOfHostName];
			double reviewsPerMonth = Double.valueOf(val[indexOfReviesPerMonth]);
			
			CustomListing customListing = new CustomListing();
			customListing.setHostName(hostName);
			customListing.setListingReviewsPerMonth(reviewsPerMonth);
			customListing.setListingUrl(listingUrl);
			customListing.setPictureUrl(pictureUrl);
			customListing.setId(id);
			Outkey.set(id);
			
			listingsMap.put(customListing,customListing.getHostName());
			
			if(listingsMap.size()>10){
				listingsMap.remove(listingsMap.firstKey());
			}
			
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text,CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(CustomListing customListing: listingsMap.keySet()){
				
				context.write(customListing,NullWritable.get());
				
			}
		}
	}
	
	
	private static class Top10ListingsByReviewsForMonthReducer extends Reducer<CustomListing,NullWritable ,CustomListing,NullWritable>{
		
		
		private TreeMap<CustomListing,String> listingsReducerMap;
		
		
		@Override
		protected void setup(Reducer<CustomListing, NullWritable,CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			listingsReducerMap = new TreeMap<>();
		}
		
		
		@Override
		protected void reduce(CustomListing key, Iterable<NullWritable> values,
				Reducer<CustomListing,NullWritable, CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			
				 listingsReducerMap.put(key,key.toString());
				 
				 if(listingsReducerMap.size()>10){
					 listingsReducerMap.remove(listingsReducerMap.firstKey());
				 }
			
			
			for(CustomListing customListing : listingsReducerMap.descendingMap().keySet()){
				
				context.write(customListing,NullWritable.get());
			}
			
		}
		
		
		@Override
		protected void cleanup(Reducer<CustomListing,NullWritable, CustomListing,NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}
	
	
	

}
