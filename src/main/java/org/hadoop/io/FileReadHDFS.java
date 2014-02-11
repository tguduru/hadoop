package org.hadoop.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Reads a file from hadoop cluster
 * 
 * @author hadoop
 * 
 */
public class FileReadHDFS {

	/**
	 * @param args
	 * @throws IOException
	 */

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out
					.println("Usage : hadoop jar <Jar> org.hadoop.io.FileReadHDFS <file-path>");
			System.exit(0);
		}
		String filePath = args[0];
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		InputStream in = fs.open(new Path(filePath));
		IOUtils.copyBytes(in, System.out, 4096);
		IOUtils.closeStream(in);
		fs.close();
	}

}
