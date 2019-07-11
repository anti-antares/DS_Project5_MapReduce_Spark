package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OaklandCrimeStatsKML extends Configured implements Tool{
    public static class OCSMap extends Mapper<LongWritable, Text, Text, IntWritable>
    {
    	// define the increment
            private final static IntWritable one = new IntWritable(1);
        // initialize the count item
            private Text word = new Text();

            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
            {
            	// get the string line
            	String line = value.toString();
            	// split them
            	String[] lineSeg = line.split("\t");
            	// if not the first line
            	if (!lineSeg[0].equals("X")) {
            		// parse its coordinates
            		double x = Double.parseDouble(lineSeg[0]);
            		double y = Double.parseDouble(lineSeg[1]);
            		// determine if it's with 200 meters of the target place
            		// 200 meters = 656.167979 feet
            		if (Math.pow(x-1354326.897, 2)+Math.pow(y-411447.7828, 2)<Math.pow(656.167979, 2)) {
            			
                    	if (lineSeg[4].equals("AGGRAVATED ASSAULT")) {
                    		// count in
                    		word.set("<Placemark>\n<Point>\n<coordinates>"+lineSeg[8]+","+lineSeg[7]+"</coordinates>\n</Point>\n</Placemark>\n");
                    		context.write(word, one); 		
                    	}
            		}


            	}
            }
    }
    
    // reducer that summarize all numbers
    public static class OCSReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
            {
            	// for each key
            	// get the counts for them
            	// and sum the counts
                    int sum = 0;
                    for(IntWritable value: values)
                    {
                            sum += value.get();
                    }
                    context.write(key, new IntWritable(sum));
            }

    }
    
    // driver method that set up mappers and reducers and make them run for the task
    public int run(String[] args) throws Exception  {
    		
    	// initialize new job
            Job job = new Job(getConf());
            job.setJarByClass(OaklandCrimeStatsKML.class);
            job.setJobName("oaklandcrimestatskml");
            
            // set output key and value classes
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            
            // set mapper and reduce classes
            job.setMapperClass(OCSMap.class);
            job.setReducerClass(OCSReducer.class);

            
            // set input and output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // set input file and output file paths
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean success = job.waitForCompletion(true);
            return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
            // start the job
            int result = ToolRunner.run(new OaklandCrimeStatsKML(), args);
            System.exit(result);
    }
}
