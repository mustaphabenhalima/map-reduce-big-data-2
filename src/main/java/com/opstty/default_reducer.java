package com.opstty;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// JAVA
import java.io.IOException;

public class default_reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    // DEFAULT REDUCER

    private IntWritable final_counter = new IntWritable(); // Instantiate the final counter

    public void reduce(Text key, Iterable<IntWritable> counters, Context context) throws IOException,InterruptedException{
        int sum = 0; // Summation variable

        for (IntWritable count : counters) {
            sum += count.get();
        }

        final_counter.set(sum); // Set final counter

        context.write(key, final_counter); // Write the key with its counter
    }
}
