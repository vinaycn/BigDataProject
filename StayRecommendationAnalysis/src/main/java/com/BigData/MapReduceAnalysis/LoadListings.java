package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.BigData.utils.ColumnParser;
import com.BigData.utils.HBaseTablesName;
import com.me.Bigdata.CustomWritable.CustomListing;
//import com.me.Bigdata.CustomWritable.CustomListing;

public class LoadListings {

	public static void main(String[] args) throws Exception {

		// Setting SYSO for the purpose of debug
		/*File f = new File("/home/vinay/Desktop/LoadListings");
		try {
			System.setOut(new PrintStream(f));
		} catch (Exception e) {

		}*/

		// First Configuration
		Configuration conf = HBaseConfiguration.create();
		conf.set("Place", "Newyork");
		Job job = Job.getInstance(conf, "LoasListings");

		job.setJarByClass(LoadListings.class);
		job.setMapperClass(LoadListingsMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		
		job.addCacheFile(new URI("/StayAnalysis/Input/Cache/NewyorkListingsHeaders#NewyorkListingsHeaders"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameforNewyorkListings,LoadListingsReducer.class, job);
		job.waitForCompletion(true);

		// Second configuration
		Configuration chicagoConf = HBaseConfiguration.create();
		chicagoConf.set("Place", "Chicago");
		Job job2 = Job.getInstance(chicagoConf, "LoadListingsChicago");
		job2.setJarByClass(LoadListings.class);
		job2.setMapperClass(LoadListingsMapper.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.addCacheFile(new URI("/StayAnalysis/Input/Cache/ChicagoListingsHeaders#ChicagoListingsHeaders"));
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameforChicagoListings, LoadListingsReducer.class,
				job2);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

	protected static class LoadListingsMapper extends Mapper<LongWritable, Text, IntWritable, MapWritable> {

		
		int indexOfReviesPerMonth;
		int indexOfListingId;
		int indexOfListingUrl;
		int indexOfHostName;
		int indexOfPictureUrl;

		String[] headerList;
		

		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, MapWritable>.Context context)
				throws IOException, InterruptedException {

			try {
				BufferedReader bufferedReader;
				String city = context.getConfiguration().get("Place", "Newyork");
				if (city.equals("Chicago")) {
					System.out.println("Doing Chicago");
					bufferedReader = new BufferedReader(new FileReader("ChicagoListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfReviesPerMonth = ColumnParser.getTheIndexOfTheColumn(headerList, "reviews_per_month");
					indexOfListingId = ColumnParser.getTheIndexOfTheColumn(headerList, "id");
					indexOfPictureUrl = ColumnParser.getTheIndexOfTheColumn(headerList, "picture_url");
					indexOfHostName = ColumnParser.getTheIndexOfTheColumn(headerList, "host_name");
					indexOfListingUrl = ColumnParser.getTheIndexOfTheColumn(headerList, "listing_url");
				} else {
					System.out.println("Doing Newyork");
					bufferedReader = new BufferedReader(new FileReader("NewyorkListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfReviesPerMonth = ColumnParser.getTheIndexOfTheColumn(headerList, "reviews_per_month");
					indexOfListingId = ColumnParser.getTheIndexOfTheColumn(headerList, "id");
					indexOfPictureUrl = ColumnParser.getTheIndexOfTheColumn(headerList, "picture_url");
					indexOfHostName = ColumnParser.getTheIndexOfTheColumn(headerList, "host_name");
					indexOfListingUrl = ColumnParser.getTheIndexOfTheColumn(headerList, "listing_url");
				}

			} catch (Exception e) {
				System.out.println("Something Went Wrong");
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val[] = value.toString().split("\t");
			if (value.toString().contains("price") || val.length != headerList.length)
				return;

			try {
				MapWritable mapWritable = new MapWritable();
				CustomListing customListing = new CustomListing();
				customListing.setListingUrl(val[indexOfListingUrl]);
				customListing.setPictureUrl(val[indexOfPictureUrl]);
				customListing.setHostName(val[indexOfHostName]);
				customListing.setListingReviewsPerMonth(Double.valueOf(val[indexOfReviesPerMonth]));
				customListing.setId(Integer.valueOf(val[indexOfListingId]));
				
				mapWritable.put(new IntWritable(customListing.getId()), customListing);
				context.write(new IntWritable(customListing.getId()), mapWritable);
			} catch (Exception e) {
				System.out.println("Something went Wrong Missing this Record");
			}

		}

	}

	

	protected static class LoadListingsReducer extends TableReducer<IntWritable,MapWritable,ImmutableBytesWritable> {

		

		Connection connection;
		Admin admin;
		String city;
		Table table;

		@Override
		protected void setup(Reducer<IntWritable,MapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			city = context.getConfiguration().get("Place");

			System.out.println(city);
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();
			String tableName = HBaseTablesName.tableNameforNewyorkListings;

			if (city.equals("Chicago")) {
				tableName = HBaseTablesName.tableNameforChicagoListings;
			} else {
				tableName = HBaseTablesName.tableNameforNewyorkListings;
			}

			if (admin.tableExists(TableName.valueOf(tableName))) {
				// Use the created table if its already exists to check the
				// column family
				table = connection.getTable(TableName.valueOf(tableName));

			} else {

				HTableDescriptor tableforThisJob = new HTableDescriptor(TableName.valueOf(tableName));

				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("ListingDescription");
				HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("ListingReviews");

				tableforThisJob.addFamily(hColumnDescriptor);
				tableforThisJob.addFamily(hColumnDescriptor1);
				admin.createTable(tableforThisJob);

			}
		}

		@Override
		protected void reduce(IntWritable key, Iterable<MapWritable> values,
				Reducer<IntWritable,MapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			Put putTolistingDetails = new Put(Bytes.toBytes(key.get()));

			for (MapWritable mapWritable : values) {
				CustomListing customListing =(CustomListing)mapWritable.get(key);
				putTolistingDetails.addColumn(Bytes.toBytes("ListingDescription"), Bytes.toBytes("ListingUrl"),
						Bytes.toBytes(customListing.getListingUrl()));
				putTolistingDetails.addColumn(Bytes.toBytes("ListingDescription"), Bytes.toBytes("PictureUrl"),
						Bytes.toBytes(customListing.getPictureUrl()));
				putTolistingDetails.addColumn(Bytes.toBytes("ListingDescription"), Bytes.toBytes("HostName"),
						Bytes.toBytes(customListing.getHostName()));
				putTolistingDetails.addColumn(Bytes.toBytes("ListingReviews"), Bytes.toBytes("ReviewsPerMonth"),
						Bytes.toBytes(customListing.getListingReviewsPerMonth()));
			}

			context.write(null, putTolistingDetails);

		}

		@Override
		protected void cleanup(Reducer<IntWritable,MapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			if(connection != null)
				connection.close();
			if(table != null)
				table.close();
		}

	}
}
