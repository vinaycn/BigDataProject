import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by vinay on 4/16/17.
 */
public class AverageAnalysis {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvaerageAnalysis");
        job.setJarByClass(AverageAnalysis.class);
        job.setMapperClass(AverageAnalysisMapper.class);

        job.setReducerClass(AverageAnalysisReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setCombinerClass(AverageAnalysisCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //cleanUpOutputDiectory(conf, args[1]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class AverageAnalysisMapper extends Mapper<Object, Text, Text, SortedMapWritable> {


        private Text outKey = new Text();
        private LongWritable one = new LongWritable(1);


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String val[] = value.toString().split(",");

            //Get the index of the room_type column
            int indexOfRoom = ColumnParser.getTheIndexOfTheColumn(value.toString().split(","), "room_type");


            //Get the index of the price column
            int indexOfprice = ColumnParser.getTheIndexOfTheColumn(value.toString().split(","), "price");
            SortedMapWritable outValue = new SortedMapWritable();
            DoubleWritable doubleWritable = new DoubleWritable(Double.valueOf(val[indexOfprice]));
            outValue.put(doubleWritable, one);
            outKey.set(val[indexOfRoom]);
            context.write(outKey, outValue);


        }
    }


    private static class AverageAnalysisCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
        @Override
        protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

            SortedMapWritable valOut = new SortedMapWritable();
            for (SortedMapWritable map : values) {

                Set<Map.Entry<WritableComparable, Writable>> entrySet = map.entrySet();
                Map.Entry<WritableComparable, Writable> next = entrySet.iterator().next();

                LongWritable presentValue = (LongWritable) valOut.get((DoubleWritable) next.getKey());

                if (presentValue == null) {
                    valOut.put(next.getKey(), next.getValue());
                } else {
                    valOut.put(next.getKey(),
                            new LongWritable(presentValue.get() + ((LongWritable) (next.getValue())).get()));
                }

                map.clear();
            }
            context.write(key, valOut);
        }

    }


    private static class AverageAnalysisReducer extends Reducer<Text, SortedMapWritable, Text, DoubleWritable> {


        final List<Double> priceList = new ArrayList<Double>();
        //private Text outkey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

            priceList.clear();
            double sum = 0;
            double count = 0;
            double average = 0;
            //outkey.set(key);
            outValue.set(0);

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
            average = sum / count;
            outValue.set(average);
            context.write(key, outValue);


        }
    }

}
