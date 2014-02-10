/**
 * 
 */
package org.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author hadoop
 *
 */
public class MaxTemparatureMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		System.out.println(" LINE ==== " + line);
		String year = line.substring(0, 4);
		System.out.println("YEAR === " + year);
		
		int lastIndex = line.lastIndexOf(',');
		int maxTemp = Integer.parseInt(line.substring(lastIndex + 1));
		output.collect(new Text(year), new IntWritable(maxTemp));
	}
	
}
