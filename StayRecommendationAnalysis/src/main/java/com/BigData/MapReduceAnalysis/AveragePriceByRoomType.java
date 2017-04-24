package com.BigData.MapReduceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import oracle.hadoop.loader.examplesLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//import com.BigData.utils.CSVInputFormat;
import com.BigData.utils.*;

public class AveragePriceByRoomType {
	
	

	public static void main(String[] args) throws Exception {
		
		//Setting SYSO for the purpose of debug
			File f = new File("/home/vinay/Desktop/outputForAveragePriceByRoomType");
			try {
				System.setOut(new PrintStream(f));
			} catch (Exception e) {

			}
		
		
		// First Configuration
		Configuration conf = HBaseConfiguration.create();
		conf.set("Place", "Newyork");
		Job job = Job.getInstance(conf, "AvaerageAnalysisByPriceByRoomTypeForNewyork");

		job.setJarByClass(AveragePriceByRoomType.class);
		job.setMapperClass(AveragePriceByRoomTypeMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		job.addCacheFile(new URI("/StayAnalysis/Input/Cache/NewyorkListingsHeaders#NewyorkListingsHeaders"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				AveragePriceByRoomTypeReducer.class, job);

		job.waitForCompletion(true);

		// Second configuration
		Configuration chicagoConf = HBaseConfiguration.create();
		chicagoConf.set("Place", "Chicago");
		Job job2 = Job.getInstance(chicagoConf, "AvaerageAnalysisByPriceByRoomTypeForChicago");
		job2.setJarByClass(AveragePriceByNoOfRooms.class);
		job2.setMapperClass(AveragePriceByRoomTypeMapper.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(SortedMapWritable.class);
		job2.addCacheFile(new URI("/StayAnalysis/Input/Cache/ChicagoListingsHeaders#ChicagoListingsHeaders"));
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob(HBaseTablesName.tableNameForAnalysisOfListingByPlace,
				AveragePriceByRoomTypeReducer.class, job2);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

	private static class AveragePriceByRoomTypeMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {

		private Text outKey = new Text();
		private LongWritable one = new LongWritable(1);
		private int indexOfRoom;
		private int indexOfprice;

		String[] headerList;
		// String place;
		

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, SortedMapWritable>.Context context)
				throws IOException, InterruptedException {

			// Read the Header txt file from the Distributed Cache for Mapreduce
			// 2
			try {
				BufferedReader bufferedReader;
				String city = context.getConfiguration().get("Place","Newyork");
				if (city.equals("Chicago")) {
					System.out.println("Doing Chicago");
					bufferedReader = new BufferedReader(new FileReader("ChicagoListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfprice = ColumnParser.getTheIndexOfTheColumn(headerList, "price");
					indexOfRoom = ColumnParser.getTheIndexOfTheColumn(headerList, "room_type");
				}
				else{
					System.out.println("Doing Newyork");
					bufferedReader = new BufferedReader(new FileReader("NewyorkListingsHeaders"));
					headerList = bufferedReader.readLine().split("\t");
					indexOfprice = ColumnParser.getTheIndexOfTheColumn(headerList, "price");
					indexOfRoom = ColumnParser.getTheIndexOfTheColumn(headerList, "room_type");
				}

			}catch (Exception e) {
				System.out.println("Something Went Wrong");
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String val[] = value.toString().split("\t");
			if (value.toString().contains("price") || val.length > 95)
				return;

			SortedMapWritable outValue = new SortedMapWritable();
			DoubleWritable doubleWritable;
			try {
				doubleWritable = new DoubleWritable(Double.valueOf(val[indexOfprice].replace('$', ' ').trim()));
			} catch (Exception e) {
				return;
			}
			outValue.put(doubleWritable, one);
			outKey.set(val[indexOfRoom]);
			context.write(outKey, outValue);
		}

	}

	/*
	 * private static class AveragePriceByRoomTypeCombiner extends
	 * TableReducer<Text, SortedMapWritable, Text> {
	 * 
	 * @Override protected void reduce(Text key, Iterable<SortedMapWritable>
	 * values, Context context) throws IOException, InterruptedException { //
	 * TODO Auto-generated method stub final SortedMapWritable valOut = new
	 * SortedMapWritable(); for (SortedMapWritable map : values) {
	 * 
	 * Set<Map.Entry<WritableComparable, Writable>> entrySet = map.entrySet();
	 * Map.Entry<WritableComparable, Writable> next =
	 * entrySet.iterator().next();
	 * 
	 * LongWritable presentValue = (LongWritable) valOut.get((DoubleWritable)
	 * next.getKey());
	 * 
	 * if (presentValue == null) { valOut.put(next.getKey(), next.getValue()); }
	 * else { valOut.put(next.getKey(), new LongWritable(presentValue.get() +
	 * ((LongWritable) (next.getValue())).get())); }
	 * 
	 * map.clear(); } context.write(key, valOut); } }
	 */

	private static class AveragePriceByRoomTypeReducer
			extends TableReducer<Text, SortedMapWritable, ImmutableBytesWritable> {

		final List<Double> priceList = new ArrayList<Double>();
		// private Text outkey = new Text();
		// private String place;

		Connection connection;
		Admin admin;
		Table table;
		String city;

		@Override
		protected void setup(Reducer<Text, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			
			city = context.getConfiguration().get("Place");
			System.out.println(city);
			connection = ConnectionFactory.createConnection(context.getConfiguration());
			// Get Admin
			admin = connection.getAdmin();

			if (admin.tableExists(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace))) {
				// Use the created table if its already exists
				// table =
				// connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));
				table = connection.getTable(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				if (!table.getTableDescriptor().hasFamily(Bytes.toBytes("AveragePriceByRoomType"))) {

					// Create the Column family
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("AveragePriceByRoomType");

					// Add the column family
					admin.addColumn(TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace),
							hColumnDescriptor);

				}
			} else {

				// Create an Table Descriptor with table name
				HTableDescriptor tablefoThisJob = new HTableDescriptor(
						TableName.valueOf(HBaseTablesName.tableNameForAnalysisOfListingByPlace));

				// create an column descriptor for the table
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("AveragePriceByRoomType");

				// add the column family for the table
				tablefoThisJob.addFamily(hColumnDescriptor);

				// This will create an new Table in the HBase
				admin.createTable(tablefoThisJob);
			}
		}

		@Override
		protected void reduce(Text key, Iterable<SortedMapWritable> values,
				Reducer<Text, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {

			priceList.clear();
			double sum = 0;
			double count = 0;
			double averagePriceForRoomType = 0;

			for (SortedMapWritable sm : values) {
				for (Map.Entry<WritableComparable, Writable> entry : sm.entrySet()) {
					long numberOfTimes = ((LongWritable) entry.getValue()).get();

					for (long i = 0; i < numberOfTimes; i++) {
						double val = ((DoubleWritable) entry.getKey()).get();
						priceList.add(val);
						sum += val;
					}
				}
				sm.clear();
			}

			count = priceList.size();
			averagePriceForRoomType = sum / count;

			Put putTolistingsAnalyisByPlace = new Put(Bytes.toBytes(city));

			// Adding Average price for Room type into hBase table
			putTolistingsAnalyisByPlace.addColumn(Bytes.toBytes("AveragePriceByRoomType"),
					Bytes.toBytes(key.toString()), Bytes.toBytes(averagePriceForRoomType));

			// Will Write to ListingsAnalyisByPlace HBase Table
			context.write(null, putTolistingsAnalyisByPlace);

		}

		@Override
		protected void cleanup(Reducer<Text, SortedMapWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			// Close the Connection
			connection.close();

			// Close the table Connection
			// table.close();
		}
	}

	private static String[] readFile(String filePath) throws FileNotFoundException {

		BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
		List<String> placeAndHeaders = bufferedReader.lines().collect(Collectors.toList());
		String[] headerList = placeAndHeaders.get(0).toString().split("\t");
		return headerList;

	}

}
