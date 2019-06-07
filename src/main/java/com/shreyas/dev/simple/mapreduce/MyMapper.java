package com.shreyas.dev.simple.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
/**************************************************************
 *   Custom mapper class which extends Base Mapper Class
 *   Base Mapper has 4 arguments , 1st two args are Input key,
 *   Input Value and the last two are Ouput key and Output
 *   value.
 */
public class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text word   =   new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Our Custom Bussiness Logic
        //Context will be in the form key-out and value-out


        /*  Key will be the byteoffet
            Value will be complete line
            Ex 1:     This is Shreyas
                    key     => 0
                    value   => This is Shreyas

            Ex 2: Can you do me a favour by giving me your pen?

            StringTokenizer takes a sentence and converts it to words.
            itr will contain ["This","is","Shreyas"] and [""]
        */
        StringTokenizer itr =   new StringTokenizer(value.toString());
        while ( itr.hasMoreTokens() ){

            //Convert String to Text Hadoop Wrapper
            word.set(itr.nextToken());

            // Each word will be initialized with 1
            //  This => 1 is => 1 Shreyas => 1
            context.write(word,new IntWritable(1));

        }
    }
}
