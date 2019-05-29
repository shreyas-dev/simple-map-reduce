package com.shreyas.dev.simple.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        String[] otherArgs= new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length<2){
            System.err.println("Usage:wordcount <in>[<in> ... ] <out>");
            System.exit(1);
        }
        Job job=new Job(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

    }
}
