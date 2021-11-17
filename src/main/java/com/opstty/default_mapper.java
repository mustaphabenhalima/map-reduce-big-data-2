package com.opstty;

// FARAMOND Emile  BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// JAVA
import java.io.IOException;
import java.util.StringTokenizer;

public class default_mapper extends Mapper<Object, Text, Text, IntWritable>{
    // DEFAULT MAPPER

    private final static IntWritable default_counter_value = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, default_counter_value);
        }
    }
}
