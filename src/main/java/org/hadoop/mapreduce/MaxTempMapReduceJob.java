/**
 * 
 */
package org.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author hadoop
 */
public class MaxTempMapReduceJob {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Usage : hadoop jar <jar> org.hadoop.mapreduce.MaxTempMapReduceJob <input path> <output path>");
		}
		JobConf jobConf = new JobConf(MaxTempMapReduceJob.class);
		jobConf.setJobName("Max Temparature Job");

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		jobConf.setMapperClass(MaxTemparatureMapper.class);
		jobConf.setReducerClass(MaxTemparatureReducer.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(jobConf);
	}

}
