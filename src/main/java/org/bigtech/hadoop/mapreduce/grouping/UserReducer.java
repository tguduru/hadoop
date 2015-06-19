package org.bigtech.hadoop.mapreduce.grouping;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Thirupathi Reddy Guduru
 * 
 *         Modified : Sep 20, 2014
 */
public class UserReducer extends
		Reducer<UserKey, IntWritable, User, IntWritable> {

	@Override
	protected void reduce(final UserKey key,
			final Iterable<IntWritable> values, final Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		for (final IntWritable value : values) {
			counter++;
		}
		final User outputKey = new User();
		outputKey.setCountry(key.getCountry());
		outputKey.setState(key.getState());
		outputKey.setCity(key.getCity());
		outputKey.setDate(key.getDate());
		context.write(outputKey, new IntWritable(counter));
	}
}
