/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package project5.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Scanner;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Arrays;
/**
 * The java class use Java spark to analyze a txt file
 * @author Zhexin Chen (zhexinc)
 */
public class TempestAnalytics {

        // if no arguments are passed - warn the user
        public static void main(String[] args) {
            
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        
        // if argument valid - do analytics
        tempestAnalytics(args[0]);
        
    }
        /**
         * do analytics on the text file passed
         * include all functionalities from task 0 to task 5
         * @param fileName 
         */
        private static void tempestAnalytics(String fileName) {
            
            // turn off the info logs
            Logger.getLogger("org").setLevel(Level.OFF);
        
            Logger.getLogger("akka").setLevel(Level.OFF);
            
            // initialize the utilities
            
            System.setProperty("hadoop.home.dir", "C:\\hadoop");

            SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Tempest Analytics");

            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
            
            // instantiate a new JavaRDD that containes each line of the text file
            
            JavaRDD<String> inputFile = sparkContext.textFile(fileName);
            
            // instantiate a new JavaRDD that containes each word of the text file
            
            JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));
            
            // instantiate a new JavaRDD that containes each distinct words of the text file
                    
            JavaRDD<String> wordsFromFileDistinct = wordsFromFile.distinct();


            // task 0: print line amounts
            System.out.println("Task 0: "+inputFile.count()+" lines");
            
            // task 1: print word amounts
            System.out.println("Task 1: "+wordsFromFile.count()+" words");
            
            // task 2: print distinct word amounts
            System.out.println("Task 2: "+wordsFromFileDistinct.count()+" distinct words");
            
            // instantiate a new JavaPairRDD that pairs each word with digit 1
            JavaPairRDD wordsPairOne = wordsFromFile.mapToPair(t -> new Tuple2(t, 1));
            
            // task 3: save the result to the directory
            wordsPairOne.saveAsTextFile("C:\\Users\\antil\\Documents\\NetBeansProjects\\Project5\\Part_2\\TheTempestOutputDir1");
            
            // instantiate a new JavaPairRDD that pairs each word with its frequencies
            JavaPairRDD wordsPairCount = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
            
            // task 4: save the result to the directory
            wordsPairCount.saveAsTextFile("C:\\Users\\antil\\Documents\\NetBeansProjects\\Project5\\Part_2\\TheTempestOutputDir2");
            
            // task 5: ask user for search string
            System.out.println("Please input the string you want to search:");

            Scanner in = new Scanner(System.in);

            String searchString = in.nextLine();
            
            System.out.println("The followings are the lines in the file that contain \""+searchString+"\"");
            
            // search the string for the user and print the lines that containes the string
            inputFile.foreach((str) ->  {
                if (str.contains(searchString))
                System.out.println(str);});
        
    }
}
