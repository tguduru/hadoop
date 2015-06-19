package org.bigtech.hadoop.mapreduce.grouping;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Thirupathi Reddy Guduru
 * 
 *         Modified : Sep 20, 2014
 */
public class UserMapper extends
		Mapper<LongWritable, Text, UserKey, IntWritable> {

	@Override
	protected void map(final LongWritable key, final Text value,
			final Context context) throws IOException, InterruptedException {
		final String stringValue = value.toString();
		final String[] columns = stringValue.split("\t");
		final UserKey userKey = new UserKey();
		userKey.setCountry(columns[0]);
		userKey.setState(columns[1]);
		userKey.setCity(columns[2]);
		userKey.setDate(columns[3]);
		userKey.setUserName(columns[4]);

		context.write(userKey, new IntWritable(1));

	}

}
