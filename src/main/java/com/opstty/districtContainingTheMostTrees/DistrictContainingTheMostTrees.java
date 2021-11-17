package com.opstty.districtContainingTheMostTrees;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class DistrictContainingTheMostTrees {
    // A MapReduce job that get the district containing the most trees in trees.csv

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Show wiki on how to use the command if the number of necessary parameters are not enough
        if (otherArgs.length < 2) {
            System.err.println("Usage: districtContainingTheMostTrees <in> <out>");
            System.err.println("<in>  : must be a file with the same syntax as trees.csv or trees.csv itself");
            System.err.println("<out> : name of the folder to write the result of this program (must not exist)");
            System.exit(2);
        }

        BasicConfigurator.configure();

        // Create a new Jar and set the driver class(this class) as the main class of jar
        Job job = Job.getInstance(conf, "districtContainingTheMostTrees"); // Job name
        job.setJarByClass(DistrictContainingTheMostTrees.class);

        // Set Mapper to mapperDistrictContainingTheMostTrees
        job.setMapperClass(mapperDistrictContainingTheMostTrees.class);

        // Set Reducer to most_trees_district_reducer
        job.setReducerClass(reducerDistrictContainingTheMostTrees.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
