package com.opstty.maximumHeightPerKindOfTree;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// JAVA
import java.io.IOException;

public class reducerMaximumHeightPerKindOfTree extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text kind, Iterable<FloatWritable> heights, Context context) throws IOException,InterruptedException{
        float max = 0;

        for(FloatWritable height : heights){
         if( height.get() > max){
             max = height.get();
         }
        }
        context.write(kind, new FloatWritable(max));

    }
}
