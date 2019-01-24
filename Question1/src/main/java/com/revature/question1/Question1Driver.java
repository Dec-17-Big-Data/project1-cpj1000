package com.revature.question1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* 
 * Input is Gender_StatsData.csv
 */

// Question 1:
// Identify the countries where % of female graduates is less than 30%. 

public class Question1Driver {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: Question1Driver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Question1Driver.class);
		job.setJobName("Question1");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Question1Mapper.class);
		job.setReducerClass(Question1Reducer.class);

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