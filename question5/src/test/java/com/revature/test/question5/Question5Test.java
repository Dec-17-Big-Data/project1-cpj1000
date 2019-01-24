package com.revature.test.question5;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question5.Question5Mapper;
import com.revature.question5.Question5Reducer;

public class Question5Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;

	@Before
	public void setUp() {
		// Mapper
		Question5Mapper mapper = new Question5Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		// Reducer
		Question5Reducer reducer = new Question5Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		// MapReducer
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws Exception {
		// mapDriver.withInput(new LongWritable(1), new
		// Text("\"Mali\",\"MLI\",\"Literacy rate, adult female (% of females ages 15
		// and
		// above)\",\"SE.ADT.LITR.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.73528\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.89343\",\"\",\"\",\"\",\"\",\"15.9\",\"\",\"\",\"18.19091\",\"\",\"\",\"\",\"20.28793\",\"24.63679\",\"\",\"\",\"\",\"22.19578\",\"\","));
		// final List<Pair<Text, Text>> result = mapDriver.run();

		// assertThat(result).isNotNull().hasSize(13)
		// .contains(new Pair<Text, Text>(new Text("United States"), new
		// Text("1999:35.85857")));

		// Can't send false arg to ignore order?
		// mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Text> test = new ArrayList<Text>();
		test.add(new Text("2012:50.0"));
		test.add(new Text("2011:40.0"));
		reduceDriver.withInput(new Text("Mali"), test);
		reduceDriver.withOutput(new Text("Mali"), new Text("2011: 40.0%"));
		reduceDriver.withOutput(new Text("Mali"), new Text("2012: 50.0% : Change: 10.0%"));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mrDriver.withInput(new LongWritable(1), new Text(
				"\"Mali\",\"MLI\",\"Literacy rate, adult female (% of females ages 15 and above)\",\"SE.ADT.LITR.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.73528\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.89343\",\"\",\"\",\"\",\"\",\"15.9\",\"\",\"\",\"18.19091\",\"\",\"\",\"\",\"20.28793\",\"24.63679\",\"\",\"\",\"\",\"22.19578\",\"\","));
		mrDriver.withOutput(new Text("Mali"), new Text("2003: 15.9%"));
		mrDriver.withOutput(new Text("Mali"), new Text("2006: 18.19091% : Change: 0.7636366666666662%"));
		mrDriver.withOutput(new Text("Mali"), new Text("2010: 20.28793% : Change: 0.5242550000000001%"));
		mrDriver.withOutput(new Text("Mali"), new Text("2011: 24.63679% : Change: 4.348860000000002%"));
		mrDriver.withOutput(new Text("Mali"), new Text("2015: 22.19578% : Change: -0.6102525000000005%"));
		mrDriver.runTest();
	}
}
