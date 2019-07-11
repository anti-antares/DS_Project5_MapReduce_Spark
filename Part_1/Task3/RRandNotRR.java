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

/**
 * 
 * The MapReduce application count the all the words that contain substring 'rr' in a word document
 * @author Zhexin Chen (zhexinc)
 *
 */
public class RRandNotRR extends Configured implements Tool {
		
	// the mapper takes in each word, and determines whether the word contains 'rr'
	// if yes - count in the context
        public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>
        {
                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();

                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        String line = value.toString();
                        // tokenize the string line
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        while(tokenizer.hasMoreTokens())
                        {
                        		String myToken = tokenizer.nextToken();
                        		// determines whether the word contains 'rr'
                        		if(myToken.contains("rr")) {
                        			word.set("rr-words");
                                    context.write(word, one);
                        		}
                                       else{
                                       word.set("non-rr-words");
                                    context.write(word, one);
                                   }
                                
                        }
                }
        }
        
        
        // reducer receives all the work from the mappers
        // and summarize all the word counts together
        public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
        {
                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
                {
                        int sum = 0;
                        // count individual values and add them together
                        for(IntWritable value: values)
                        {
                                sum += value.get();
                        }
                        context.write(key, new IntWritable(sum));
                }

        }

        public int run(String[] args) throws Exception  {
        		
        	// start a new job
        	// set job attributes
                Job job = new Job(getConf());
                job.setJarByClass(RRandNotRR.class);
                job.setJobName("wordcount");

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
                //set mapper and reducer
                job.setMapperClass(WordCountMap.class);
                job.setReducerClass(WordCountReducer.class);


                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                
                //get inputfile and output file

                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                boolean success = job.waitForCompletion(true);
                return success ? 0: 1;
        }


        public static void main(String[] args) throws Exception {
                // run the MapReduce application
                int result = ToolRunner.run(new RRandNotRR(), args);
                // exit upon completion
                System.exit(result);
        }

}
