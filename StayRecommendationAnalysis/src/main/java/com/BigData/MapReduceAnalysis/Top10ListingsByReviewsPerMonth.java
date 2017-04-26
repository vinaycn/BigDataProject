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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.BigData.MapReduceAnalysis.LoadListings.LoadListingsMapper;
import com.BigData.MapReduceAnalysis.LoadListings.LoadListingsReducer;
import com.BigData.utils.ColumnParser;
import com.BigData.utils.HBaseTablesName;
import com.me.Bigdata.CustomWritable.CustomListing;

public class Top10ListingsByReviewsPerMonth {

	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		// Setting SYSO for the purpose of debug

		Configuration conf = HBaseConfiguration.create();
		conf.set("Place", "Newyork");
		Job job = Job.getInstance(conf, "Top10ListingsByReviewsForMonthInNewyork");

		job.setJarByClass(Top10ListingsByReviewsPerMonth.class);
		job.setMapperClass(Top10ListingsByReviewsPerMonthMapper.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.addCacheFile(new URI("/StayAnalysis/Input/Cache/NewyorkListingsHeaders#NewyorkListingsHeaders"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				Top10ListingsByReviewsForMonthReducer.class, job);
		job.waitForCompletion(true);

		// Second configuration
		Configuration chicagoConf = HBaseConfiguration.create();
		chicagoConf.set("Place", "Chicago");
		Job job2 = Job.getInstance(chicagoConf, "Top10ListingsByReviewsForMonthInChicago");
		job2.setJarByClass(Top10ListingsByReviewsPerMonth.class);
		job2.setMapperClass(Top10ListingsByReviewsPerMonthMapper.class);

		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.addCacheFile(new URI("/StayAnalysis/Input/Cache/ChicagoListingsHeaders#ChicagoListingsHeaders"));
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				Top10ListingsByReviewsForMonthReducer.class, job2);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

	private static class Top10ListingsByReviewsPerMonthMapper
			extends Mapper<Object, Text, DoubleWritable, IntWritable> {

		private TreeMap<Double, Integer> listingsMap;
		String[] headerList;
		int indexOfReviesPerMonth;
		int indexOfListingId;

		@Override
		protected void setup(Mapper<Object, Text, DoubleWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				listingsMap = new TreeMap<>();
				BufferedReader bufferedReader;
				String city = context.getConfiguration().get("Place");
				if (city.equals("Chicago")) {
					System.out.println("In Chicago Job");
					bufferedReader = new BufferedReader(new FileReader("ChicagoListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfReviesPerMonth = ColumnParser.getTheIndexOfTheColumn(headerList, "reviews_per_month");
					indexOfListingId = ColumnParser.getTheIndexOfTheColumn(headerList, "id");

				} else {
					System.out.println("In Newyork Job");
					bufferedReader = new BufferedReader(new FileReader("NewyorkListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfReviesPerMonth = ColumnParser.getTheIndexOfTheColumn(headerList, "reviews_per_month");
					indexOfListingId = ColumnParser.getTheIndexOfTheColumn(headerList, "id");

				}

			} catch (Exception e) {
				System.out.println("Something Went Wrong");
			}

		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, DoubleWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String val[] = value.toString().split("\t");
			if (value.toString().contains("price") || val.length != headerList.length)
				return;

			

			try {
				int id = Integer.valueOf(val[indexOfListingId].trim());
				double reviewsPerMonth = Double.valueOf(val[indexOfReviesPerMonth]);

				listingsMap.put(reviewsPerMonth, id);

				if (listingsMap.size() > 10) {
					listingsMap.remove(listingsMap.firstKey());
				}
			} catch (Exception e) {
				System.out.println("Something Went Wrong!");
			}

		}

		@Override
		protected void cleanup(Mapper<Object, Text, DoubleWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Double d : listingsMap.keySet()) {

				context.write(new DoubleWritable(d), new IntWritable(listingsMap.get(d)));

			}
		}
	}

	private static class Top10ListingsByReviewsForMonthReducer
			extends TableReducer<DoubleWritable, IntWritable, ImmutableBytesWritable> {

		private TreeMap<Double, Integer> listingsReducerMap;

		static {
			File f = new File("/home/vinay/Desktop/Top10Reducer");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		}

		Connection connection;
		Admin admin;
		String city;
		Table table;

		@Override
		protected void setup(Reducer<DoubleWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			listingsReducerMap = new TreeMap<>();
			city = context.getConfiguration().get("Place");

			System.out.println(city);
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();

			if (admin.tableExists(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace))) {
				// Use the created table if its already exists to check the
				// column family
				table = connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("Top10ListingsByReviews"))) {

					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("Top10ListingsByReviews");

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
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("Top10ListingsByReviews");

				// add the column family for the table
				tablefoThisJob.addFamily(hColumnDescriptor);

				// This will create an new Table in the HBase
				admin.createTable(tablefoThisJob);
			}

		}

		@Override
		protected void reduce(DoubleWritable key, Iterable<IntWritable> values,
				Reducer<DoubleWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context arg2)
				throws IOException, InterruptedException {

			for (IntWritable id : values) {
				listingsReducerMap.put(key.get(), id.get());
			}
			if (listingsReducerMap.size() > 10) {
				listingsReducerMap.remove(listingsReducerMap.firstKey());
			}

		}

		@Override
		protected void cleanup(Reducer<DoubleWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			System.out.println(city);

			Put topListingsByReviews = new Put(Bytes.toBytes(city));
			

			String[] position = { "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eigth", "Ninth",
					"Tenth" };
			int i = 0;

			System.out.println(listingsReducerMap.size());
			for (Integer id : listingsReducerMap.descendingMap().values()) {
				System.out.println(id);
				topListingsByReviews.addColumn(Bytes.toBytes("Top10ListingsByReviews"), Bytes.toBytes(position[i]),
						Bytes.toBytes(id));
				i++;

			}
			System.out.println(topListingsByReviews);
				context.write(null, topListingsByReviews);
			if (connection != null)
				connection.close();
			if (table != null)
				table.close();
		}

	}

}
