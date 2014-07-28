package com.renaissance.mrunit.hbase.tests;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
//import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.renaissance.mrunit.hbase.HBaseExpectedColumn;
import com.renaissance.mrunit.hbase.HBaseMapDriver;

public class HBaseMapDriverTests {
	private static final String TITLE_COLUMNFAMILY = "t";
	private HaikuMapper sut;
	private MapDriver<LongWritable,Text,ImmutableBytesWritable,Put> mapDriver;
	private HBaseMapDriver<LongWritable, Text, ImmutableBytesWritable> driver;

	@Rule
	  public final ExpectedException thrown = ExpectedException.none();
	
	@Before
	public void setup(){
		sut = new HaikuMapper();
		mapDriver = MapDriver.newMapDriver(sut);
		driver = new HBaseMapDriver(mapDriver);
	}

//	  @Test
//	  public void testTestRun2() throws IOException {
//	    thrown
//	        .expectAssertionErrorMessage("1 Error(s): (Expected no output; got 1 output(s).)");
//	    driver.withInput(new Text("foo"), new Text("bar")).runTest();
//	  }

	@Test
	public void oneInput_expectOneOutput() throws IOException {
		String message = null;
		try
		{
//		thrown.expectMessage("1 Error(s): (Expected no output(s); got 1 output(s).)");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("1 Error(s): (Expected no output(s); got 1 output(s).)", message);
	}
//"1 Error(s): (Missing expected output (foo, bar) at position 1.)"
	
	@Test
	public void noInput_missingRowKey() throws IOException {
		String message = null;
		try{
			HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), oldPond.Value(new Text("old pond...\na frog leaps in\nwater's sound")))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("foo")), oldPond.Value(new Text("")))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("1 Error(s): (Missing expected rowkey (foo).)", message);
	}
@Test
public void test(){
	thrown.expect(IndexOutOfBoundsException.class);
	throw new IndexOutOfBoundsException();
}
}
