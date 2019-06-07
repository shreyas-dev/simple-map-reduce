package com.shreyas.dev.simple.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();
        // Pass the cmd line args
        // We will be running it as hadoop jar java.jar in out
        // and out the convention java -jar , hence we need GenericOptionsParser
        String[] otherArgs= new GenericOptionsParser(conf,args).getRemainingArgs();


        if (otherArgs.length<2){
            System.err.println("Usage:wordcount <in>[<in> ... ] <out>");
            System.exit(1);
        }

        // Creating a hadoop MR Job
        Job job=new Job(conf,"word count");
        //Setting the Main Class which contains main method
        job.setJarByClass(WordCount.class);
        //Mapper Class
        job.setMapperClass(MyMapper.class);
        // Reducer Class
        job.setReducerClass(MyReducer.class);

        //Setting the output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        JobConf jobConf=new JobConf(conf);
        // Multiple Input Files
        for (int i=0;i<otherArgs.length-1;i++){
            FileInputFormat.addInputPath(jobConf,new Path(otherArgs[i]));
        }
        //Output file location
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));


        //job.waitForCompletion will submit the job to map-reduce
        System.exit(job.waitForCompletion(true) ? 0 : 1 );

    }
}
