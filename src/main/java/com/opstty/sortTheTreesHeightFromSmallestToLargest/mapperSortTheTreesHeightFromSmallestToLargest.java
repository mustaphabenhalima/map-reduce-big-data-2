package com.opstty.sortTheTreesHeightFromSmallestToLargest;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// JAVA
import java.io.IOException;

public class mapperSortTheTreesHeightFromSmallestToLargest extends Mapper<Object, Text, Text, FloatWritable>{
    private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (line != 0){ // Skip Header
            try{

                String[] fields = value.toString().split(";");

                Text kind = new Text(fields[11] + " - " + fields[2] + " : "); // Get the kind

                Float height = Float.parseFloat(fields[6]); // Get its height

                context.write(new FloatWritable(height), kind); // Write both of them in the context

            }catch(NumberFormatException ex){

            }

        }

        line++;
    }
}
