package com.revature.question3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Question3Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// Stores year/percent info in treemap to be sorted.
		Map<Integer, Double> data = new TreeMap<Integer, Double>();
		for (Text value : values) {
			String[] delimited = value.toString().split(":");
			data.put(Integer.parseInt(delimited[0]), Double.parseDouble(delimited[1]));
		}

		// Only returns country if it was able to found two data points to compare.
		if (data.size() >= 2) {
			Integer[] year = new Integer[2];
			Double[] percent = new Double[2];
			int count = 0;
			for (Map.Entry<Integer, Double> entry : data.entrySet()) {
				year[count] = entry.getKey();
				percent[count] = entry.getValue();
				count++;
			}
			// Change key to average change
			context.write(key,
					new Text(Integer.toString(year[0]) + "~" + Integer.toString(year[1]) + ":"
							+ Double.toString(percent[0]) + "%~" + Double.toString(percent[1]) + "%:Overall % change="
							+ Double.toString(((percent[1] / percent[0]) * 100.0D) - 100.0D) + "%:Average % change="
							+ Double.toString(
									(((percent[1] / percent[0]) * 100.0D) - 100.0D) / Double.valueOf(year[1] - year[0]))
							+ "%"));
		}
	}
}