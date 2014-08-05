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
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
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
	List<Pair<OutputKey, List<ExpectedValue>>> expectedOutputs = new ArrayList<Pair<OutputKey, List<ExpectedValue>>>();
	
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
		expectedOutputs.add(new Pair<OutputKey, List<ExpectedValue>>(key, Arrays.asList(values)));
		return this;
	}
	
	public List<Pair<OutputKey, Writable>> run() throws IOException{
		return driver.run();
	}

	public void runTest() throws IOException{
		validate(expectedOutputs, driver.run());
	}
	
	public void validate(final List<Pair<OutputKey, Writable>> outputs){
		validate(expectedOutputs, outputs);
	}
	
	private void validate(final List<Pair<OutputKey, List<ExpectedValue>>> expectedOutputs, final List<Pair<OutputKey, Writable>> outputs){
		final Errors errors = new Errors(LOG);
		compareRecordCounts(errors, expectedOutputs, outputs);
		checkForExpected(errors, expectedOutputs, outputs);
		checkForUnexpected(errors, expectedOutputs, outputs);
		errors.assertNone();
	}
	
	private void checkForExpected(
			final Errors errors,
			final List<Pair<OutputKey, List<ExpectedValue>>> expectedOutputs,
			final List<Pair<OutputKey, Writable>> outputs) {
		if (0 == expectedOutputs.size())
			return;
		for (Pair<OutputKey, List<ExpectedValue>> expected : expectedOutputs){
			String expectedKey = getExpectedKey(expected);
			ArrayList<Pair<OutputKey, Writable>> matchingRows = getActualRowsWithMatchingRowKey(expectedKey, outputs);
			if (0 == matchingRows.size())
				errors.record("Missing expected rowkey (%s).", expectedKey);
			else
				checkForExpectedColumnsInActual(errors, expected, matchingRows);
		}
	}
	
	private void checkForExpectedColumnsInActual(
			final Errors errors,
			Pair<OutputKey, List<ExpectedValue>> expectedRow,
			ArrayList<Pair<OutputKey, Writable>> matchingRows) {
		for(ExpectedValue expected : expectedRow.getSecond()){
			if (expectedNotInActual(errors, matchingRows, expected))
				errors.record(	"Missing expected column (%s:%s).", 
								Bytes.toString(expected.getColumnFamily()),
								Bytes.toString(expected.getQualifier()));
		}
	}

	private boolean expectedNotInActual(final Errors errors,
			ArrayList<Pair<OutputKey, Writable>> matchingRows,
			ExpectedValue expected) {
		for(Pair<OutputKey,Writable> actualRow : matchingRows){
			if (expectedColumnInActual(errors, expected, actualRow.getSecond()))
				return false;
		}
		return true;
	}

	private boolean expectedColumnInActual(final Errors errors,
			ExpectedValue expected, Writable actual) {
		if (actual instanceof Put)
			return expectedColumnInActual(errors, expected, (Put)actual);
		//TODO: Add a case for KeyValue.
		return false;
	}
	
	private boolean expectedColumnInActual(final Errors errors,
			ExpectedValue expected, Put actual) {
		if (actual.has(expected.getColumnFamily(), expected.getQualifier())) {
			String actualValue = getActualValue(actual, expected);
			if (false == expected.getExpected().equals(actualValue))
				errors.record(	"Mismatch value for: Basho(t:new pond)\t\tExpected: %s\t\tRecieved: %s",
								expected.getExpected(),
								actualValue);
			return true;
		}
		return false;
	}

	private ArrayList<Pair<OutputKey, Writable>> getActualRowsWithMatchingRowKey(
			String expectedKey, List<Pair<OutputKey, Writable>> outputs) {
		ArrayList<Pair<OutputKey, Writable>> matchingRows = new ArrayList<Pair<OutputKey, Writable>>();
		for(Pair<OutputKey, Writable> actual : outputs){
			if (expectedKey.equals(getActualKey(actual)))
				matchingRows.add(actual);
		}
		return matchingRows;
	}

	private void checkForUnexpected(
			final Errors errors,
			final List<Pair<OutputKey, List<ExpectedValue>>> expectedOutputs,
			final List<Pair<OutputKey, Writable>> outputs) {
		if (0 == outputs.size())
			return;
		for(Pair<OutputKey, Writable> actual : outputs){
			String actualKey = getActualKey(actual);
			ArrayList<Pair<OutputKey, List<ExpectedValue>>> matchingRows = getMatchingExpectedRows(actualKey, expectedOutputs);
			if (0 == matchingRows.size())
				errors.record("Recieved unexpected rowkey (%s).", actualKey);
			else
				checkForUnexpectedColumns(errors, actual, matchingRows);
		}
	}
	
	private void checkForUnexpectedColumns(Errors errors,
			Pair<OutputKey, Writable> actual,
			ArrayList<Pair<OutputKey, List<ExpectedValue>>> matchingRows) {
		Put actualColumn = (Put)actual.getSecond();
		if (false == expectedRowsContainsActual(matchingRows, actualColumn))
			recordUnexpectedColumns(errors, actualColumn);
	}

	private boolean expectedRowsContainsActual(
			ArrayList<Pair<OutputKey, List<ExpectedValue>>> matchingRows,
			Put actualColumn) {
		//.net trueForAll() with predicate...
		for(Pair<OutputKey, List<ExpectedValue>> expectedRow : matchingRows)
			if (expectedColulmnsContainsActual(actualColumn, expectedRow))
				return true;
		return false;
	}

	private boolean expectedColulmnsContainsActual(Put actualColumn,
			Pair<OutputKey, List<ExpectedValue>> expectedRow) {
		//.net trueForAll() with a predicate...
		for(ExpectedValue expectedColumn : expectedRow.getSecond())
			if (actualColumn.has(expectedColumn.getColumnFamily(), expectedColumn.getQualifier()))
				return true;
		return false;
	}

	private void recordUnexpectedColumns(Errors errors, Put actualColumn) {
		for(List<KeyValue> columnList : actualColumn.getFamilyMap().values())
			for(KeyValue column : columnList)
				errors.record(	"Recieved unexpected column (%s:%s).", 
								Bytes.toString(column.getFamily()), 
								Bytes.toString(column.getQualifier()));
	}

	private ArrayList<Pair<OutputKey, List<ExpectedValue>>> getMatchingExpectedRows(
			String actualKey,
			List<Pair<OutputKey, List<ExpectedValue>>> expectedRows) {
		//Still a .Net boy at heart. This would be a List<T>.Find() and not a whole method... sigh
		ArrayList<Pair<OutputKey, List<ExpectedValue>>> matchingRows = new ArrayList<Pair<OutputKey, List<ExpectedValue>>>();
		for(Pair<OutputKey, List<ExpectedValue>> expected : expectedRows){
			String expectedKey = getExpectedKey(expected);
			if (actualKey.equals(expectedKey))
				matchingRows.add(expected);
		}
		return matchingRows;
	}

	private void compareRecordCounts(
			final Errors errors,
			final List<Pair<OutputKey, List<ExpectedValue>>> expectedOutputs,
			final List<Pair<OutputKey, Writable>> outputs) {
	    if (!outputs.isEmpty() && expectedOutputs.isEmpty()) 
	    	errors.record("Expected no output(s); got %d output(s).", outputs.size());
	}

	private String getActualValue(Put writable, ExpectedValue expectedColumn) {
		return Bytes.toString(writable.get(expectedColumn.getColumnFamily(), expectedColumn.getQualifier()).get(0).getValue());
	}

	private String getActualKey(final Pair<OutputKey, Writable> actual) {
		return getKeyAsString(actual.getFirst());
	}

	private String getExpectedKey(Pair<OutputKey, List<ExpectedValue>> expected) {
		return getKeyAsString(expected.getFirst());
	}
	
	private String getKeyAsString(OutputKey key){
		String result = key.toString();
		if (key instanceof ImmutableBytesWritable)
			result = Bytes.toString(((ImmutableBytesWritable)key).get());
		return result;
	}
}
