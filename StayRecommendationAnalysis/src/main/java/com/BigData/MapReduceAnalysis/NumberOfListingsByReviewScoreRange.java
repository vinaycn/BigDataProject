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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.BigData.utils.ColumnParser;
import com.BigData.utils.HBaseTablesName;

public class NumberOfListingsByReviewScoreRange {

	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		// Setting SYSO for the purpose of debug

		Configuration conf = HBaseConfiguration.create();
		conf.set("Place", "Newyork");
		Job job = Job.getInstance(conf, "NumberOfListingsByReviewScoreRange");

		job.setJarByClass(NumberOfListingsByReviewScoreRange.class);
		job.setMapperClass(NumberOfListingsByReviewScoreRangeMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);
		// job.addCacheFile(new
		// URI("/StayAnalysis/Input/Cache/NewyorkListingsHeaders#NewyorkListingsHeaders"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				NumberOfListingsByReviewScoreRangeReducer.class, job);
		job.waitForCompletion(true);

		// Second configuration
		Configuration chicagoConf = HBaseConfiguration.create();
		chicagoConf.set("Place", "Chicago");
		Job job2 = Job.getInstance(chicagoConf, "NumberOfListingsByReviewScoreRangeInChicago");
		job2.setJarByClass(NumberOfListingsByReviewScoreRange.class);
		job2.setMapperClass(NumberOfListingsByReviewScoreRangeMapper.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setNumReduceTasks(4);
		// job2.addCacheFile(new
		// URI("/StayAnalysis/Input/Cache/ChicagoListingsHeaders#ChicagoListingsHeaders"));
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				NumberOfListingsByReviewScoreRangeReducer.class, job2);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

	private static class NumberOfListingsByReviewScoreRangeMapper
			extends Mapper<Object, Text, IntWritable, IntWritable> {

		IntWritable outKey = new IntWritable();
		IntWritable outValue = new IntWritable();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String val[] = value.toString().split("\t");

			try {
				int reviewScore = Integer.valueOf(val[0]);
				int numOfListings = Integer.valueOf(val[1]);
				outKey.set(reviewScore);
				outValue.set(numOfListings);
				context.write(outKey, outValue);
			} catch (Exception e) {
				System.out.println("Something Went Wrong");
			}

		}

	}

	private static class NumberOfListingsByReviewScoreRangePartitioner extends Partitioner<IntWritable, IntWritable> {

		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			if (key.get() > 0 && key.get() <= 90)
				return 0;
			else if (key.get() > 90 && key.get() <= 95)
				return 1;
			else if (key.get() > 95 && key.get() <= 99)
				return 2;
			else
				return 3;
		}

	}

	private static class NumberOfListingsByReviewScoreRangeReducer
			extends TableReducer<IntWritable, IntWritable, ImmutableBytesWritable> {

		Connection connection;
		Admin admin;
		String city;
		Table table;
		double numberOfListings;

		@Override
		protected void setup(Reducer<IntWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			city = context.getConfiguration().get("Place");

			System.out.println(city);
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();

			if (admin.tableExists(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace))) {
				// Use the created table if its already exists to check the
				// column family
				table = connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("NumberOfLitingsByReviewScoreRange"))) {

					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("NumberOfLitingsByReviewScoreRange");

					// Add the column family
					admin.addColumn(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace),
							hColumnDescriptor);
					System.out.println("Creating Columns");

				}
			} else {

				// Create an Table Descriptor with table name
				HTableDescriptor tablefoThisJob = new HTableDescriptor(
						TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				// create an column descriptor for the table
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("NumberOfLitingsByReviewScoreRange");

				// add the column family for the table
				tablefoThisJob.addFamily(hColumnDescriptor);

				// This will create an new Table in the HBase
				admin.createTable(tablefoThisJob);
			}

		}

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			Put listingsByReviewRange = new Put(Bytes.toBytes(city));
			
			for(IntWritable nl : values){
				numberOfListings =+ nl.get();
			}

			if (key.get() > 0 && key.get() <= 90) {
				listingsByReviewRange.addColumn(Bytes.toBytes("NumberOfLitingsByReviewScoreRange"),
						Bytes.toBytes("0To90(Reveiew Score)"), Bytes.toBytes(numberOfListings));

			} else if (key.get() > 90 && key.get() <= 95) {
				listingsByReviewRange.addColumn(Bytes.toBytes("NumberOfLitingsByReviewScoreRange"),
						Bytes.toBytes("90To95 (Reveiew Score)"), Bytes.toBytes(numberOfListings));

			} else if (key.get() > 95 && key.get() <= 99) {
				listingsByReviewRange.addColumn(Bytes.toBytes("NumberOfLitingsByReviewScoreRange"),
						Bytes.toBytes("95To99 (Reveiew Score)"), Bytes.toBytes(numberOfListings));

			} else {
				listingsByReviewRange.addColumn(Bytes.toBytes("NumberOfLitingsByReviewScoreRange"),
						Bytes.toBytes("100 (Reveiew Score)"), Bytes.toBytes(numberOfListings));
			}
			context.write(null, listingsByReviewRange);

		}

		@Override
		protected void cleanup(Reducer<IntWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			
			if(connection != null)
				connection.close();
			if(table != null)
				table.close();

		}

	}

}
