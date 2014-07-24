/*
 * Software License Agreement for MRUnit HBase Extension
 * 
 * Copyright (c) 2014 Renaissance Learning, Inc. and James Argeropoulos
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.renaissance.mrunit.hbase.tests;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.renaissance.mrunit.hbase.HBaseExpectedColumn;
import com.renaissance.mrunit.hbase.HBaseMapDriver;

public class SampleUseTest {
	private static final String TITLE_COLUMNFAMILY = "t";
	private HaikuMapper sut;
	private MapDriver<LongWritable,Text,ImmutableBytesWritable,Put> mapDriver;
	private HBaseMapDriver<LongWritable, Text, ImmutableBytesWritable> driver;
	
	@Before
	public void setup(){
		sut = new HaikuMapper();
		mapDriver = MapDriver.newMapDriver(sut);
		driver = new HBaseMapDriver(mapDriver);
	}
	
	@Test
	public void oneHaiku_splitAndAddedToHbase() throws IOException {
		HBaseExpectedColumn oldPond = new HBaseExpectedColumn(TITLE_COLUMNFAMILY, "old pond");
		driver.withInput(new LongWritable(0L), new Text("Basho\nold pond\nold pond...\na frog leaps in\nwater's sound"))
				.withOutput(new ImmutableBytesWritable(Bytes.toBytes("Basho")), oldPond.Value(new Text("old pond...\na frog leaps in\nwater's sound")))
				.runTest();
	}

}
