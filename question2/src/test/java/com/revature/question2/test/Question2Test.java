package com.revature.question2.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.question2.map.Question2Mapper;
import com.revature.question2.reduce.Question2Reducer;

public class Question2Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;

	@Before
	public void setUp() {
		// Mapper
		Question2Mapper mapper = new Question2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		// Reducer
		Question2Reducer reducer = new Question2Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		// MapReducer
		mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws Exception {
		// mapDriver.withInput(new LongWritable(1), new Text(
		// "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female
		// (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
		// final List<Pair<Text, Text>> result = mapDriver.run();

		// Can't import junit to use assertThat

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
		reduceDriver.withInput(new Text("United States"), test);
		reduceDriver.withOutput(new Text("United States"), new Text("2011: 40.0%"));
		reduceDriver.withOutput(new Text("United States"), new Text("2012: 50.0% : Change: 10.0%"));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		mrDriver.withInput(new LongWritable(1), new Text(
				"\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\","));
		mrDriver.withOutput(new Text("United States"), new Text("2000: 37.8298%"));
		mrDriver.withOutput(new Text("United States"), new Text("2001: 37.43131% : Change: -0.39848999999999535%"));
		mrDriver.withOutput(new Text("United States"), new Text("2002: 38.22037% : Change: 0.7890599999999992%"));
		mrDriver.withOutput(new Text("United States"), new Text("2003: 39.18913% : Change: 0.9687599999999961%"));
		mrDriver.withOutput(new Text("United States"), new Text("2004: 39.84185% : Change: 0.6527200000000022%"));
		mrDriver.withOutput(new Text("United States"), new Text("2005: 40.23865% : Change: 0.39679999999999893%"));
		mrDriver.withOutput(new Text("United States"), new Text("2006: 41.26198% : Change: 1.0233300000000014%"));
		mrDriver.withOutput(new Text("United States"), new Text("2007: 42.00725% : Change: 0.7452699999999979%"));
		mrDriver.withOutput(new Text("United States"), new Text("2008: 42.78946% : Change: 0.7822099999999992%"));
		mrDriver.withOutput(new Text("United States"), new Text("2009: 43.68347% : Change: 0.8940100000000015%"));
		mrDriver.withOutput(new Text("United States"), new Text("2011: 46.37914% : Change: 1.347835%"));
		mrDriver.withOutput(new Text("United States"), new Text("2012: 47.68032% : Change: 1.3011800000000022%"));
		mrDriver.runTest();
	}
}
