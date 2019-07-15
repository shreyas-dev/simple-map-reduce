package com.shreyas.dev.simple.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf=new JobConf();

        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length < 2 ){
            System.err.println("Usage: wordcount <in> [<in> ...] <out>");
            System.exit(2);
        }
        //Job Name
        conf.setJobName("wordcount");
        //Setting the output key and value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        //Input file location
        FileInputFormat.setInputPaths(conf, new Path(otherArgs[0]));

        //Output file location
        FileOutputFormat.setOutputPath(Job.getInstance(conf), new Path(otherArgs[1]));
        Job job=new Job(conf);

        //Setting the Main Class which contains main method
        job.setJarByClass(WordCount.class);
        //Mapper Class
        job.setMapperClass(MyMapper.class);
        // Reducer Class
        job.setReducerClass(MyReducer.class);

        //Setting the output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.waitForCompletion will submit the job to map-reduce
        System.exit(job.waitForCompletion(true) ? 0 : 1 );

    }
}
