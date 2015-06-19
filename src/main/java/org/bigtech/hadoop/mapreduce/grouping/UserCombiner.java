package org.bigtech.hadoop.mapreduce.grouping;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Thirupathi Reddy Guduru
 * 
 *         Modified : Sep 20, 2014
 */
public class UserCombiner extends
		Reducer<UserKey, IntWritable, UserKey, IntWritable> {
	@Override
	protected void reduce(final UserKey key,
			final Iterable<IntWritable> values, final Context context)
			throws IOException, InterruptedException {
		// final User userKey = new User();
		// userKey.setCountry(key.getCountry());
		// userKey.setState(key.getState());
		// userKey.setCity(key.getCity());
		// userKey.setDate(key.getDate());
		context.write(key, new IntWritable(1));
	}
}
