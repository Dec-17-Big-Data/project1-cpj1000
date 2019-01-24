package com.revature.question5;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Question5Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// Stores year/percent info in treemap to be sorted.
		Map<Integer, Double> data = new TreeMap<Integer, Double>();
		for (Text value : values) {
			String[] delimited = value.toString().split(":");
			data.put(Integer.parseInt(delimited[0]), Double.parseDouble(delimited[1]));
		}

		// variables to calculate average change in percent per year
		Integer older_year = null;
		Double older_percent = null;
		Double change = null;

		// iterates through the year data from oldest to newest. Calculates the average
		// change in literacy per year.
		for (Map.Entry<Integer, Double> entry : data.entrySet()) {
			Integer year = entry.getKey();
			Double percent = entry.getValue();
			if (older_year == null || older_percent == null) {
				older_year = year;
				older_percent = percent;
				context.write(key, new Text(year + ": " + percent + "%"));
			} else {
				change = (percent - older_percent) / Double.valueOf(year - older_year);
				context.write(key, new Text(year + ": " + percent + "% : " + "Change: " + change + "%"));
				older_year = year;
				older_percent = percent;
			}
		}
	}
}