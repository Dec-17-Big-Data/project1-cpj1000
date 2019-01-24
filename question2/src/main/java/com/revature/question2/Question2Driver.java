package com.revature.question2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.question2.map.Question2Mapper;
import com.revature.question2.reduce.Question2Reducer;

/* 
 * Input is Gender_StatsData.csv
 */

// Question 2:
// List the average increase in female education in the U.S. from the year 2000.

public class Question2Driver {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: Question2Driver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Question2Driver.class);
		job.setJobName("Question2");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Question2Mapper.class);
		job.setReducerClass(Question2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}