package org.bigtech.hadoop.mapreduce.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Thirupathi Reddy Guduru
 * 
 *         Modified : Sep 20, 2014
 */
public class UserGroupingComparator extends WritableComparator {

	/**
	 * @param keyClass
	 * @param createInstances
	 */
	public UserGroupingComparator() {
		super(UserKey.class, true);
	}

	@Override
	public int compare(final WritableComparable a, final WritableComparable b) {
		final UserKey key1 = (UserKey) a;
		final UserKey key2 = (UserKey) b;
		return key1.getCountry().equals(key2.getCountry())
				&& key1.getState().equals(key2.getState())
				&& key1.getCity().equals(key2.getCity())
				&& key1.getDate().equals(key2.getDate()) ? 0 : 1;
	}
}
