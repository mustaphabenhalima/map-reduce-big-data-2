package com.opstty.numberOfTreesByKinds;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// JAVA
import java.io.IOException;

public class mapperNumberOfTreesByKinds extends Mapper<Object, Text, Text, IntWritable> {
    private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        if (line != 0){ // Skip Header
            IntWritable default_counter = new IntWritable(1); // Instantiate the default counter

            Text word = new Text(value.toString().split(";+")[2]); // Get the kind

            context.write(word, default_counter); // Write it
        }

        line++;
    }
}
