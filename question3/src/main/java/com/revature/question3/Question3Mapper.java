package com.revature.question3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Pil Ju Chun
 *
 */
public class Question3Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String object.
		 */
		String line = value.toString();
		/*
		 * Regex checks for "," (quotation mark punctuation included between entries,
		 * first index entry leading " and last index entry trailing " must be trimmed.
		 */
		String[] entries = line.split("\",\"?", -1);

		/*
		 * Find rows with indicator code "SE.TER.CMPL.FE.ZS", which has the indicator
		 * name "Tertiary education, gross completion ratio, female (ISCED 5A first
		 * degree)
		 */

		if (entries[3].equals("SL.EMP.TOTL.SP.MA.ZS")) {
			// Trim leading " punctuation
			String countryName = entries[0].substring(1);
			// Need to get two data points, employment at 2000 (close as possible) and
			// newest possible date
			// and calculate the % change from those two points. Should also print average %
			// annual.
			for (int i = 0; i < 16; i++) {
				String temp = entries[entries.length - (1 + i)];
				int year = 2016 - i;
				if (temp != null && !temp.equals("")) {
					context.write(new Text(countryName), new Text(year + ":" + temp));
					// Breaks for loop when data found from right end
					i = 100;
				}
			}

			for (int i = 0; i < 16; i++) {
				String temp = entries[entries.length - 17 + i];
				int year = 2000 + i;
				if (temp != null && !temp.equals("")) {
					context.write(new Text(countryName), new Text(year + ":" + temp));
					// Breaks for loop when data found from right end
					i = 100;
				}
			}
		}
	}
}
