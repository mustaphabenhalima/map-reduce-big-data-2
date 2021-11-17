package com.opstty.districtContainingTheMostTrees;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

// JAVA
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class reducerDistrictContainingTheMostTrees extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private HashMap<Integer, Integer> list = new HashMap<>();

    public void reduce(IntWritable key, Iterable<IntWritable> counters, Context context){
        int sum = 0; // Summation variable

        for (IntWritable count : counters) {
            sum += count.get();
        }

        this.list.put(key.get(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Integer maxKey = null;
        Integer maxValue = 0;

        for(Map.Entry<Integer, Integer> district : list.entrySet()){
            if(district.getValue() > maxValue){
                maxKey = district.getKey();
                maxValue = district.getValue();
            }
        }

        context.write(new IntWritable(maxKey), new IntWritable(maxValue));
    }
}
