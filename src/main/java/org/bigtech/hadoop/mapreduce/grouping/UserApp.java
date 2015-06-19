package org.bigtech.hadoop.mapreduce.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Thirupathi Reddy Guduru
 * 
 *         Modified : Sep 20, 2014
 */
public class UserApp extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.println("Usage : hadoop jar <jar> UserApp <input path> <output path>");
		}
		final int res = ToolRunner
				.run(new Configuration(), new UserApp(), args);
		System.exit(res);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = this.getConf();

		final Job job = new Job(conf, "Tool Job");
		job.setJarByClass(UserApp.class);

		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReducer.class);

		job.setMapOutputKeyClass(UserKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(User.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setCombinerClass(UserCombiner.class);
		job.setGroupingComparatorClass(UserGroupingComparator.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
