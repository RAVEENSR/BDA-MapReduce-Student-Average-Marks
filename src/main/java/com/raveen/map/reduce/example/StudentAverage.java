package com.raveen.map.reduce.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class uses map reduce technique to calculate each student average mark.
 */
public class StudentAverage {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /* first element represents the student id and the rest represents marks of each module.
               ex: 09SIKOSR,3,51,68,35,31
            */
            String[] studentDetails = value.toString().split(",");

            String studentId = studentDetails[0];
            Text studentIdKey = new Text(studentId);
            String[] studentMarks = Arrays.copyOfRange(studentDetails, 1, 6);
            for (String mark: studentMarks) {
                IntWritable markValue = new IntWritable(Integer.parseInt(mark));
                context.write(studentIdKey, markValue);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text studentIdKey, Iterable<IntWritable> marks, Context context)
                throws IOException, InterruptedException {
            int sumOfMarks = 0;
            int numberOfModules = 0;
            for (IntWritable mark : marks) {
                numberOfModules += 1;
                sumOfMarks += mark.get();
            }
            int avgOfMarks = sumOfMarks/numberOfModules;
            context.write(studentIdKey, new IntWritable(avgOfMarks));
        }
    }

    public static void main(String[] args) throws Exception {
        // Uncomment the following to generate the data set
        // GenerateDataSet.generateStudentMarks(1000000);

        Configuration conf = new Configuration();

        Job job = new Job(conf);

        job.setJarByClass(StudentAverage.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
