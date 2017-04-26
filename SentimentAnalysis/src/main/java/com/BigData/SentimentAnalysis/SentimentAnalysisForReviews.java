package com.BigData.SentimentAnalysis;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SentimentAnalysisForReviews {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Listings Review Sentiment Analysis");
			job.setJarByClass(SentimentAnalysisForReviews.class);
			job.setMapperClass(ListingsReviewsSentimentAnalysisMapper.class);
			job.setReducerClass(ListingsReviewsSentimentAnalysisReducer.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// cleanUpOutputDiectory(conf, args[1]);
			System.exit(job.waitForCompletion(false) ? 0 : 1);
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		}
	}

	public static class ListingsReviewsSentimentAnalysisMapper extends Mapper<Object,Text,NullWritable,IntWritable> {

		static {
			File f = new File("/home/vinay/Desktop/ReviewSentimentAnalysisMapper");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}
		
		/*NaiveBayes nv;
		
		@Override
		protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.nv = new NaiveBayes("/home/vinay/Desktop/mahoutSentiment/all-labelled-data");
			try {
				nv.trainModel();
				System.out.println("Training Model");
			} catch (Exception e) {
				System.out.println("Error While Training");
			}
		}*/
		
		/*@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// System.out.println("In Mapper");
			String[] split = value.toString().split("\t");
			// System.out.println("In Mapper" + split.length);
			if (split.length != 6) {
				return;
			}
			IntWritable keyOut = new IntWritable();
			Text outValue = new Text();
			try {
				keyOut.set(Integer.parseInt(split[0]));
				outValue.set(split[5]);
				// valueOut.set(nv.classifyNewReview(split[5]));
			} catch (Exception e) {
				// TODO: handle exception
			}
			context.write(keyOut, outValue);
		}*/
		
		NaiveBayes nv;

		@Override
		protected void setup(Mapper<Object, Text,NullWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.nv = new NaiveBayes("/home/vinay/Desktop/mahoutSentiment/all-labelled-data");
			try {
				nv.trainModel();
			} catch (Exception e) {
				System.out.println(e);
			}
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			if (split.length != 6) {
				return;
			}
			//IntWritable keyOut = new IntWritable();
			IntWritable valueOut = new IntWritable();
			try {
				//keyOut.set(Integer.parseInt(split[0]));
				valueOut.set(nv.classifyNewReview(split[5]));
			} catch (Exception e) {
				// TODO: handle exception
			}
			context.write(NullWritable.get(), valueOut);
		}
	}

	public static class ListingsReviewsSentimentAnalysisReducer
			extends Reducer<NullWritable, IntWritable,Text, DoubleWritable> {

		static {
			File f = new File("/home/vinay/Desktop/ReviewSentimentAnalysisReducer");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}

		
		/*NaiveBayes nv;
		@Override
		protected void setup(Reducer<IntWritable, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			System.out.println("IN REducer");
			this.nv = new NaiveBayes("/home/vinay/Desktop/mahoutSentiment/all-labelled-data");
			try {
				nv.trainModel();
				System.out.println("Training Model");
			} catch (Exception e) {
				System.out.println("Error While Training");
			}

		}*/

		/*@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			// TODO Auto-generated method stub

			List<String> listOfReviews = new ArrayList<>();

			for (Text review : values) {

				listOfReviews.add(review.toString());
				if (listOfReviews.size() > 10)
					break;

			}
			int countOfPositiveReviews = 0;

			for (String review : listOfReviews) {
				int reviewSentiment = nv.classifyNewReview(review);
				countOfPositiveReviews += reviewSentiment;
			}
			double percentageOfPositiveReviews = ((float) countOfPositiveReviews / listOfReviews.size()) * 100;
			double negativeReviewPercentage = 100 - percentageOfPositiveReviews;

			System.out.print("Listings Id " + key.get());
			System.out.print("Pos  " + percentageOfPositiveReviews);
			System.out.print("Neg  " + negativeReviewPercentage);
			System.out.println();

			context.write(key, new DoubleWritable(percentageOfPositiveReviews));

		}*/
		
		
		@Override
		protected void reduce(NullWritable keyIn, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int numberOfReviews = 0;
			int countOfPositiveReviews = 0;
			for (IntWritable val : values) {
				countOfPositiveReviews += val.get();
				numberOfReviews = numberOfReviews + 1;
			}
			float percentageOfPositiveReviews = ((float) countOfPositiveReviews / numberOfReviews) * 100;
			context.write(new Text("Chicago"), new DoubleWritable(percentageOfPositiveReviews));
		}

	}

}
