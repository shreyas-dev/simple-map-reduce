package com.shreyas.dev.simple.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /*
            key=> Shreyas
            Value => [1 1 1 1 1 1 .....]
         */


        int sum=0;
        // Loop for adding all the values.
        for (IntWritable val:values){
            sum+=val.get();
        }

        // key=> Shreyas ,sum=>12
        context.write(key,new IntWritable(sum));

    }
}
