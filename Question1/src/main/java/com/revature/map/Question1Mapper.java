package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Pil Ju Chun
 *
 */
public class Question1Mapper extends Mapper<LongWritable, Text, Text, Text> {

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

		if (entries[3].equals("SE.TER.CMPL.FE.ZS")) {
			// Trim leading " punctuation
			String countryName = entries[0].substring(1);
			// writes from rightmost column data first and skips country name, country code,
			// indicator name, and indicator code.
			for (int i = entries.length - 1; i > 3; i--) {
				if (!entries[i].equals("")) {
					// write format is <"countryName", "year - graduate percentage">
					context.write(new Text(countryName), new Text((1956 + i) + ":" + entries[i]));
				}
			}
		}
	}
}
