package com.opstty.sortTheTreesHeightFromSmallestToLargest;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// JAVA
import java.io.IOException;

public class reducerSortTheTreesHeightFromSmallestToLargest extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    public void reduce(FloatWritable height, Iterable<Text> numberOfTreesByKinds, Context context) throws IOException,InterruptedException{
        for(Text kind : numberOfTreesByKinds){
            context.write(kind, height); // Write the kind and its height
        }
    }
}
