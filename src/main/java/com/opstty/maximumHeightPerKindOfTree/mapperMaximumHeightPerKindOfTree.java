package com.opstty.maximumHeightPerKindOfTree;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapperMaximumHeightPerKindOfTree extends Mapper<Object, Text, Text, FloatWritable> {
    private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {


        if (line != 0){ // Skip Header
            String[] fields = value.toString().split(";");
            Text kind = new Text(fields[2]); // Get the kind

            Float height = new Float(0);
            try{
                height = Float.parseFloat(fields[6]); // Get its height

            }catch(NumberFormatException ex){

            }
            context.write(kind, new FloatWritable(height)); // Write both of them in the context
        }

        line++;
    }
}

