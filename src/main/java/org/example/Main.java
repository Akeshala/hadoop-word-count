package org.example;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Main.class); // job config for map reduce job
        conf.setJobName("WordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        // The Mapper class processes the input data and generates key-value pairs.
        // Each Mapper takes a single input split and processes it, producing intermediate key-value pairs.
        // In the Word Count example, the Mapper typically reads a line of text, splits it into words,
        // and emits each word as a key with a count of one.

        conf.setCombinerClass(WordCountReducer.class);
        // The Combiner class is an optimization step that reduces the amount of data transferred between the Mapper and Reducer.
        // It acts as a mini-reducer that runs after the Mapper to combine intermediate outputs before sending them to the actual Reducer.
        // This helps in reducing the volume of data shuffled across the network.


        conf.setReducerClass(WordCountReducer.class);
        // The Reducer class aggregates the intermediate data produced by the Mapper (or Combiner) to produce the final output.
        // Each Reducer takes a set of intermediate key-value pairs and merges them to produce the final result.
        // In the Word Count example, the Reducer sums up the counts of each word to get the total occurrences.

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
    }
}