package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Question2Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		boolean write = false;
		int latest_year = Integer.MIN_VALUE;
		double latest_percent = -1.0D;
		for (Text value : values) {
			String[] data = value.toString().split(":");
			int year = Integer.parseInt(data[0]);
			double percent = Double.parseDouble(data[1]);
			if (year >= latest_year && percent < 30.0D) {
				write = true;
				latest_year = year;
				latest_percent = percent;
			} else if (year < latest_year && percent >= 30.0D) {
				write = false;
			}
		}

		if (write) {
			context.write(key, new Text(latest_year + ": " + latest_percent + "%"));
		}
	}
}