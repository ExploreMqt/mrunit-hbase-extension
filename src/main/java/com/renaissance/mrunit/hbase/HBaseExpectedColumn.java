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

import org.apache.hadoop.io.Writable;

import com.renaissance.mrunit.hbase.HBaseExpectedColumn;

/*
 * A helper class for creating the expected data. Instantiate one of these objects for each column & qualifier that you expect to be in the output. 
 * 	HBaseExpectedColumn gradeColumn = new HBaseExpectedColumn(METRIC_COLUMNFAMILY, GRADE_QUALIFIER);
 * 
 * In your withOutput() method use it to add the expected data.
 * .withOutput(new ImmutableBytesWritable(Bytes.toBytes("expectedRowKey")), gradeColumn.Value(new Text("9"))) 
 */
public class HBaseExpectedColumn{
	private byte[] columnFamily;
	private byte[] qualifier;
	public HBaseExpectedColumn(String columnFamily, String qualifier){
		this.columnFamily = columnFamily.getBytes();
		this.qualifier = qualifier.getBytes();
	}
	public ExpectedValue Value(Writable value){
		return new ExpectedValue(columnFamily, qualifier, value);
	}
	public class ExpectedValue{
		private byte[] columnFamily;
		private byte[] qualifier;
		private String expected;
		public ExpectedValue(byte[] cf, byte[] q, Writable val){
			columnFamily = cf;
			qualifier = q;
			expected = val.toString();
		}
		public byte[] getColumnFamily(){
			return columnFamily;
		}
		public byte[] getQualifier(){
			return qualifier;
		}
		public String getExpected(){
			return expected;
		}		
	}
}