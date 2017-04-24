package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
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

		Job job = Job.getInstance(conf, "ListingsReviewSentimentAnalysis");
		conf.set("Place", "Berlin");
		job.setJarByClass(ListingsReviewSentimentAnalysis.class);
		job.setMapperClass(ReviewsSentimentAnalysisMapper.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ReviewSentimentScore.class);
		job.setReducerClass(ReviewSentimentAnalysisReducer.class);
		// job.setCombinerClass(AveragePriceByRoomTypeCombiner.class);
		job.addCacheFile(new URI("/Stay/Cache/negativeWords.txt#negativeWords"));
		job.addCacheFile(new URI("/Stay/Cache/positiveWords.txt#positiveWords"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForReviewsAnalysis,
				//ReviewSentimentAnalysisReducer.class, job);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class ReviewsSentimentAnalysisMapper extends Mapper<Object, Text, LongWritable, Text> {

		

		@Override
		protected void setup(Mapper<Object, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
		}

		LongWritable reviewId = new LongWritable();
		Text review = new Text();
		
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String val[] = value.toString().split("\t");
			if (value.toString().contains("listing_id") || val.length != 6 )
				return;
			
			//Take the review Id
			 reviewId.set(Long.valueOf(val[1]));
			
			 //Get the review
			 review.set(val[5].toString());
			 
			 context.write(reviewId,review);
			
			
		}

		@Override
		protected void cleanup(Mapper<Object, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}

	public static class ReviewSentimentAnalysisReducer extends Reducer<LongWritable, Text, LongWritable,ReviewSentimentScore> {
		
		
		Map<String, String> negativeWordsHashMap = new HashMap<String, String>();
		Map<String, String> positiveWordsHashMap = new HashMap<String, String>();
		
		@Override
		protected void setup(Reducer<LongWritable, Text, LongWritable, ReviewSentimentScore>.Context context)
				throws IOException, InterruptedException {
			BufferedReader bufferedNegativeReader = new BufferedReader(new FileReader("negativeWords"));
			BufferedReader bufferedPositiveReader = new BufferedReader(new FileReader("positiveWords"));
			while (bufferedPositiveReader.readLine() != null) {

				positiveWordsHashMap.put(bufferedPositiveReader.readLine(), bufferedPositiveReader.readLine());
			}
			while (bufferedNegativeReader.readLine() != null) {
				negativeWordsHashMap.put(bufferedNegativeReader.readLine(), bufferedNegativeReader.readLine());

			}
		}
		
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, LongWritable, ReviewSentimentScore>.Context context) throws IOException, InterruptedException {
			
			
               for(Text review : values){
            	   
            	   int score = getSentimentScore(review.toString());
            	   ReviewSentimentScore rs = new ReviewSentimentScore();
            	   rs.setReview(review.toString());
            	   rs.setSentimentScore(score);
            	   context.write(key, rs);
               }
			
		}
		
		
		
		private  int getSentimentScore(String input) {
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
				if (positiveWordsHashMap.get(words[i])!= null) {
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
