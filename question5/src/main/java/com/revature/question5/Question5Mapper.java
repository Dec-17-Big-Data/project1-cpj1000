package com.revature.question5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Pil Ju Chun
 * 
 *         Question 5 Check the change in literacy rate in Mali on adult women
 *         ages 15 and above
 */
public class Question5Mapper extends Mapper<LongWritable, Text, Text, Text> {

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
		 * Find rows with indicator code "SE.ADT.LITR.FE.ZS", which is literacy rate,
		 * adult female (% of females ages 15 and above)
		 */
		if (entries[0].substring(1).equals("Mali")) {
			if (entries[3].equals("SE.ADT.LITR.FE.ZS")) {
				// Trim leading " punctuation
				String countryName = entries[0].substring(1);
				// writes from rightmost column data first and skips country name, country code,
				// indicator name, and indicator code and years below 2000.
				for (int i = entries.length - 1; i > 43; i--) {
					if (!entries[i].equals("")) {
						// write format is <"countryName", "year - literacy percentage">
						context.write(new Text(countryName), new Text((1956 + i) + ":" + entries[i]));
					}
				}
			}
		}
	}
}
