package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.BigData.utils.HBaseTablesName;
import com.me.Bigdata.CustomWritable.ReviewSentimentScore;

public class ListingsReviewSentimentAnalysis {

	public static void main(String[] args) throws URISyntaxException, IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();

		Job job = Job.getInstance(conf, "Listings Review Sentiment Analysis");
		job.setJarByClass(ListingsReviewSentimentAnalysis.class);
		job.setMapperClass(ReviewsSentimentAnalysisMapper.class);
		// job.setReducerClass(SentimentAnalysisReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.addCacheFile(new URI("/StayAnalysis/Input/Cache/negativeWords.txt#negativeWords"));
		job.addCacheFile(new URI("/StayAnalysis/Input/Cache/positiveWords.txt#positiveWords"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameforChicagoListings,
				ReviewSentimentAnalysisReducer.class, job);

		System.exit(job.waitForCompletion(false) ? 0 : 1);

	}

	public static class ReviewsSentimentAnalysisMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {

		}

		IntWritable listingId = new IntWritable();
		Text review = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String val[] = value.toString().split("\t");
			if (value.toString().contains("listing_id") || val.length != 6)
				return;

			try {
				// Take the review Id
				listingId.set(Integer.valueOf(val[0]));

				// Get the review
				review.set(val[5].toString());

				context.write(listingId, review);
			} catch (Exception e) {
				System.out.println("Something Went Wrong");
			}

		}

	}

	public static class ReviewSentimentAnalysisReducer extends TableReducer<IntWritable, Text, ImmutableBytesWritable> {

		static {
			File f = new File("/home/vinay/Desktop/ReviewSentimentAnalysisReducerToken");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}

		Map<String, String> negativeWordsHashMap = new HashMap<String, String>();
		Map<String, String> positiveWordsHashMap = new HashMap<String, String>();
		Connection connection;
		Admin admin;
		// String city;
		Table table;

		@Override
		protected void setup(Reducer<IntWritable, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			BufferedReader bufferedNegativeReader = new BufferedReader(new FileReader("negativeWords"));
			BufferedReader bufferedPositiveReader = new BufferedReader(new FileReader("positiveWords"));
			while (bufferedPositiveReader.readLine() != null) {

				positiveWordsHashMap.put(bufferedPositiveReader.readLine(), bufferedPositiveReader.readLine());
			}
			while (bufferedNegativeReader.readLine() != null) {
				negativeWordsHashMap.put(bufferedNegativeReader.readLine(), bufferedNegativeReader.readLine());

			}

			// city = context.getConfiguration().get("Place");

			// System.out.println(city);
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();
			String tableName = HBaseTablesName.tableNameforChicagoListings;

			if (admin.tableExists(TableName.valueOf(tableName))) {
				// Use the created table if its already exists to check the
				// column family
				table = connection.getTable(TableName.valueOf(tableName));
				System.out.println("Table Exists");
				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("ReviewsSentimentAnalyis"))) {

					System.out.println("No coulumn Creating Column");
					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("ReviewsSentimentAnalyis");

					// Add the column family
					admin.addColumn(TableName.valueOf(HBaseTablesName.tableNameforChicagoListings), hColumnDescriptor);
				}

			} else {

				HTableDescriptor tableforThisJob = new HTableDescriptor(TableName.valueOf(tableName));

				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("ReviewsSentimentAnalyis");

				tableforThisJob.addFamily(hColumnDescriptor);

				admin.createTable(tableforThisJob);

			}
			
			System.out.println(" Postive " +positiveWordsHashMap.size());
			System.out.println(" Negative " +negativeWordsHashMap.size());
		}

		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			int numberOfPositiveReviews = 0;
			int numberOfNegativeReviews = 0;
			int numberOfNeutralReviews = 0;
			int totalReviews = 0;
			for (Text review : values) {
				totalReviews++;
				int score = getSentimentScore(review.toString());
				if (score == 1) {
					numberOfPositiveReviews++;
				} else if (score < 1) {
					numberOfNegativeReviews++;
				} else {
					numberOfNeutralReviews++;
				}

			}

			double percentageOfPositiveReviews = (numberOfPositiveReviews / totalReviews) * 100;
			double neutralPercentageReviews = (numberOfNeutralReviews / totalReviews) * 100;
			double negativeReviewPercentage = 100 - (percentageOfPositiveReviews + neutralPercentageReviews);

			Put listingsReviewsSentimentScore = new Put(Bytes.toBytes(key.get()));

			listingsReviewsSentimentScore.addColumn(Bytes.toBytes("ReviewsSentimentAnalyis"),
					Bytes.toBytes("PositiveReviewPercentage"), Bytes.toBytes(percentageOfPositiveReviews));
			listingsReviewsSentimentScore.addColumn(Bytes.toBytes("ReviewsSentimentAnalyis"),
					Bytes.toBytes("NegativeReviewPercentage"), Bytes.toBytes(negativeReviewPercentage));
			listingsReviewsSentimentScore.addColumn(Bytes.toBytes("ReviewsSentimentAnalyis"),
					Bytes.toBytes("NeutralReviewPercentage"), Bytes.toBytes(neutralPercentageReviews));

			System.out.println(percentageOfPositiveReviews);
			context.write(null, listingsReviewsSentimentScore);

		}

		@Override
		protected void cleanup(Reducer<IntWritable, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			if (connection != null)
				connection.close();
			if (table != null)
				table.close();
		}

		private int getSentimentScore(String input) {
			// normalize!
			input = input.toLowerCase();
			input = input.trim();
			// remove all non alpha-numeric non whitespace chars
			input = input.replaceAll("[^a-zA-Z0-9\\s]", "");

			int negCounter = 0;
			int posCounter = 0;

			// so what we got?
			String[] words = input.split(" ");

			// check if the current word appears in our reference lists...
			for (int i = 0; i < words.length; i++) {
				if (positiveWordsHashMap.get(words[i]) != null) {
					posCounter++;
				}
				if (negativeWordsHashMap.get(words[i]) != null) {
					negCounter++;
				}
			}

			// positive matches MINUS negative matches
			int result = (posCounter - negCounter);

			// negative?
			if (result < 0) {
				return -1;
				// or positive?
			} else if (result > 0) {
				return 1;
			}

			// neutral to the rescue!
			return 0;
		}

	}

}
