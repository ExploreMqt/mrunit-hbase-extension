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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HaikuMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private static final byte[] TITLE_COLUMNFAMILY = Bytes.toBytes("t");
	private static final int THIRD_LINE = 4;
	private static final int SECOND_LINE = 3;
	private static final int FIRST_LINE = 2;
	private static final int TITLE = 1;
	private static final int AUTHOR = 0;
	ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\n");
//		if (lines.length < 5)
//			return;
		String haiku = String.format("%s\n%s\n%s", lines[FIRST_LINE], lines[SECOND_LINE], lines[THIRD_LINE]);
		rowkey.set(Bytes.toBytes(lines[AUTHOR]));
		Put data = new Put(rowkey.get());
		data.add(TITLE_COLUMNFAMILY, Bytes.toBytes(lines[TITLE]), Bytes.toBytes(haiku));
		context.write(rowkey, data);
	}
}
