package com.revature.test.question3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question3.Question3Mapper;
import com.revature.question3.Question3Reducer;

public class Question3Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;

	@Before
	public void setUp() {
		// Mapper
		Question3Mapper mapper = new Question3Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		// Reducer
		Question3Reducer reducer = new Question3Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		// MapReducer
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(
				"\"Arab World\",\"ARB\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"68.1179873182358\",\"67.0545908162867\",\"67.0085755999808\",\"67.0539468453704\",\"66.4148811246575\",\"66.4198567531598\",\"66.44116294572\",\"66.2761947738386\",\"67.1654204331319\",\"66.6027239773966\",\"66.4269586854107\",\"65.9516809309819\",\"65.7791865398789\",\"66.551285171425\",\"67.2190774799568\",\"67.056023704773\",\"67.2885425850511\",\"67.7612145210072\",\"68.3400901200106\",\"68.9677375091437\",\"68.1293766385998\",\"68.035421015871\",\"68.2159750354989\",\"68.0686218477244\",\"68.1397504589243\",\"68.2349169615356\","));
		mapDriver.withOutput(new Text("Arab World"), new Text("2015:68.2349169615356"));
		mapDriver.withOutput(new Text("Arab World"), new Text("2000:66.4269586854107"));
		mapDriver.runTest();

	}

	@Test
	public void testReducer() {
		List<Text> value = new ArrayList<Text>();
		value.add(new Text("2010:100"));
		value.add(new Text("2000:10"));
		reduceDriver.withInput(new Text("Arab World"), value);
		reduceDriver.withOutput(new Text("Arab World"),
				new Text("2000~2010:10.0%~100.0%:Overall % change=900.0%:Average % change=90.0%"));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
	}
}
