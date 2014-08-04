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

	@Test
	public void expectMoreOutput_missingRowKey() throws IOException {
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
	public void expectTwoOutput_missingRowKeys() throws IOException {
		String message = null;
		try{
			HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), oldPond.Value(new Text("old pond...\na frog leaps in\nwater's sound")))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("foo")), oldPond.Value(new Text("")))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("bar")), oldPond.Value(new Text("")))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("2 Error(s): (Missing expected rowkey (foo)., Missing expected rowkey (bar).)", message);
	}

	@Test
	public void oneInputs_noOutput_unexpectedRow() throws IOException {
		String message = null;
		try{
			driver.withInput(new LongWritable(0L), new Text("Soseki\nOver the wintery\nOver the wintry\nforest, winds howl in rage\nwith no leaves to blow."))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("2 Error(s): (Expected no output(s); got 1 output(s)., Recieved unexpected rowkey (Soseki).)", message);
	}

	@Test
	public void twoInputs_noOutput_unexpectedRow() throws IOException {
		String message = null;
		try{
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withInput(new LongWritable(0L), new Text("Soseki\nOver the wintery\nOver the wintry\nforest, winds howl in rage\nwith no leaves to blow."))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("3 Error(s): (Expected no output(s); got 2 output(s)., Recieved unexpected rowkey (Basho)., Recieved unexpected rowkey (Soseki).)", message);
	}

	@Test
	public void oneInput_unexpectedColumn() throws IOException {
		String message = null;
		try{
			HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), new HBaseExpectedColumn.ExpectedValue[]{})
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("1 Error(s): (Recieved unexpected column (t:old pond).)", message);
	}

	@Test
	public void oneInput_missingColumn() throws IOException {
		String message = null;
		try{
			HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
			HBaseExpectedColumn newPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "new pond");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), oldPond.Value(new Text("old pond...\na frog leaps in\nwater's sound")))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), newPond.Value(new Text("")))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("1 Error(s): (Missing expected column (t:new pond).)", message);
	}

	@Test
	public void expectedValueDoesNotMatchActual() throws IOException {
		String message = null;
		try{
			HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
			driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
					.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), oldPond.Value(new Text("old pond...\na frog leaps out\nwater's sound")))
					.runTest();
		}
		catch(AssertionError e){
			message = e.getMessage();
		}
		assertEquals("1 Error(s): (Mismatch value for: Basho(t:new pond)\t\tExpected: old pond...\na frog leaps out\nwater's sound\t\tRecieved: old pond...\na frog leaps in\nwater's sound)", message);
	}
	

@Test
public void test(){
	thrown.expect(IndexOutOfBoundsException.class);
	throw new IndexOutOfBoundsException();
}
}
