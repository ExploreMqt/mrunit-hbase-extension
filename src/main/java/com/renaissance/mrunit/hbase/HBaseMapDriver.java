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
package com.renaissance.mrunit.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.internal.util.Errors;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;

import com.renaissance.mrunit.hbase.HBaseExpectedColumn.ExpectedValue;

/*
 * This class wraps the standard MRUnit.MapDriver object. It provides it's own validate method so that we can have meaningful comparisons of actual vs expected.
 * Instantiate the MapDriver as you normally would. Then instantiate an HBaseMapDriver with the built in instance. Use the wrapper class for all of the remainder of your test activities.
 * When the test is run, the wrapper's enhanced validate method will use the expected data to verify the work.
 */
public class HBaseMapDriver<InputKey, InputValue, OutputKey> {
	public static final Log LOG = LogFactory.getLog(HBaseMapDriver.class);
	MapDriver<InputKey, InputValue, OutputKey, Writable> driver;
	List<Pair<OutputKey, List<ExpectedValue>>> expectedResults = new ArrayList<Pair<OutputKey, List<ExpectedValue>>>();
	
	public HBaseMapDriver(MapDriver<InputKey, InputValue, OutputKey, Writable> adapted){
		driver = adapted;
	}

	public HBaseMapDriver<InputKey, InputValue, OutputKey> withInput(InputKey key, InputValue value) {
		driver.addInput(key, value);
		return this;
	}

	public HBaseMapDriver<InputKey, InputValue, OutputKey> withInput(InputKey key, List<InputValue> values) {
		for(InputValue value: values){
			driver.addInput(key, value);
		}
		return this;
	}
	
	public HBaseMapDriver<InputKey, InputValue, OutputKey> withOutput(OutputKey key, ExpectedValue... values){
		expectedResults.add(new Pair<OutputKey, List<ExpectedValue>>(key, Arrays.asList(values)));
		return this;
	}
	
	public List<Pair<OutputKey, Writable>> run() throws IOException{
		return driver.run();
	}

	public void runTest() throws IOException{
		validate(expectedResults, driver.run());
	}
	
	private void validate(final List<Pair<OutputKey, List<ExpectedValue>>> expectedResults, final List<Pair<OutputKey, Writable>> actuals){
		final Errors errors = new Errors(LOG);
		compareRecordCounts(errors, expectedResults, actuals);
		int i= 0;
		for(Pair<OutputKey, List<ExpectedValue>> expected : expectedResults){
			if (i == actuals.size())
				break;
			final Pair<OutputKey, Writable> actual = actuals.get(i++);
			compareKeys(errors, expected, actual);
			compareValues(errors, expected, actual);
		}
		errors.assertNone();
	}

	private void compareRecordCounts(
			final Errors errors,
			final List<Pair<OutputKey, List<ExpectedValue>>> expectedResults,
			final List<Pair<OutputKey, Writable>> actuals) {
		if(expectedResults.size() != actuals.size()) {
			 errors.record("Mismatch in output size.  Expected %s got %s", expectedResults.size(), actuals.size());
		}
	}

	private void compareValues(final Errors errors,
			Pair<OutputKey, List<ExpectedValue>> expected,
			final Pair<OutputKey, Writable> actual) {
		Put writable = (Put)actual.getSecond();
		for(ExpectedValue expectedColumn : expected.getSecond()){
			try
			{
				String actualValue = getActualValue(writable, expectedColumn);
				if (!expectedColumn.getExpected().equals(actualValue))
					errors.record("Mapper value does not match expected result.  Expected '%s' got '%s'", expectedColumn.getExpected(), actualValue);
			}
			catch (IndexOutOfBoundsException e){
				errors.record("Could not find a column for %s:%s", new String(expectedColumn.getColumnFamily()), new String(expectedColumn.getQualifier()));
			}
		}
	}

	private String getActualValue(Put writable, ExpectedValue expectedColumn) {
		return Bytes.toString(writable.get(expectedColumn.getColumnFamily(), expectedColumn.getQualifier()).get(0).getValue());
	}

	private void compareKeys(final Errors errors,
			Pair<OutputKey, List<ExpectedValue>> expected,
			final Pair<OutputKey, Writable> actual) {
		String expectedKey = getExpectedKey(expected);
		String actualKey = getActualKey(actual);
		if(!expectedKey.endsWith(actualKey))
			errors.record("Mapper key does not match expected result.  "
					+ "Expected '%s' got '%s'", expectedKey, actualKey);
	}

	private String getActualKey(final Pair<OutputKey, Writable> actual) {
		String actualKey = actual.getFirst().toString();
		if (actual.getFirst() instanceof ImmutableBytesWritable)
			actualKey = Bytes.toString(((ImmutableBytesWritable)actual.getFirst()).get());
		return actualKey;
	}

	private String getExpectedKey(Pair<OutputKey, List<ExpectedValue>> expected) {
		String expectedKey = expected.getFirst().toString();
		if (expected.getFirst() instanceof ImmutableBytesWritable)
			expectedKey = Bytes.toString(((ImmutableBytesWritable)expected.getFirst()).get());
		return expectedKey;
	}
}
