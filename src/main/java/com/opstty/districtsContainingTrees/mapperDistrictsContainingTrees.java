package com.opstty.districtsContainingTrees;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// JAVA
import java.io.IOException;

public class mapperDistrictsContainingTrees extends Mapper<Object, Text, Text, IntWritable>{
    private int line = 0;

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {

        if (line != 0){ // Skip Header
            context.write(new Text(value.toString().split(";")[1]), new IntWritable(1));
        }

        line++;
    }


}
