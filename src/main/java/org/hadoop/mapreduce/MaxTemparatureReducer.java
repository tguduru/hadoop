/**
 * 
 */
package org.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author hadoop
 *
 */
public class MaxTemparatureReducer implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
			throws IOException {
		int maxTemp = Integer.MIN_VALUE;
		while(values.hasNext()){
			maxTemp = Math.max(maxTemp, values.next().get());
		}
		outputCollector.collect(key, new IntWritable(maxTemp));
	}

}
