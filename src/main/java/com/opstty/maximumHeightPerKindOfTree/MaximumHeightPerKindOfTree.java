package com.opstty.maximumHeightPerKindOfTree;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class MaximumHeightPerKindOfTree {
    // A MapReduce job that get the height of the maximumHeightPerKindOfTree tree of each kind in trees.csv
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Show wiki on how to use the command if the number of necessary parameters are not enough
        if (otherArgs.length < 2) {
            System.err.println("Usage: maximumHeightPerKindOfTree <in> <out>");
            System.err.println("<in>  : must be a file with the same syntax as trees.csv or trees.csv itself");
            System.err.println("<out> : name of the folder to write the result of this program (must not exist)");
            System.exit(2);
        }

        BasicConfigurator.configure();

        Job job = Job.getInstance(conf, "maximumHeightPerKindOfTree"); // Job name

        job.setJarByClass(MaximumHeightPerKindOfTree.class);

        // Set Mapper to mapperMaximumHeightPerKindOfTree
        job.setMapperClass(mapperMaximumHeightPerKindOfTree.class);

        // Set Combiner and Reducer to maximumHeightPerKindOfTree_reducer
        job.setCombinerClass(reducerMaximumHeightPerKindOfTree.class);
        job.setReducerClass(reducerMaximumHeightPerKindOfTree.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
