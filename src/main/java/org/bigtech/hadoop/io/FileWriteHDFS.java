/**
 * 
 */
package org.bigtech.hadoop.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * Writes a local file into hadoop cluster
 * @author hadoop
 *
 */
public class FileWriteHDFS {
	public static void main(String args[]) throws IOException, URISyntaxException{
		if(args.length != 2){
			System.out.println("Usage : hadoop jar <Jar> FileWriteHDFS <local-file-path> <hdfs-path> ");
			System.exit(0);
		}
		String localPath = args[0];
		String hdfsPath = args[1];
		InputStream inputStream = new BufferedInputStream(new FileInputStream(localPath));
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI(hdfsPath), configuration);
		OutputStream outputStream = fs.create(new Path(hdfsPath), new Progressable() {
			
			@Override
			public void progress() {
			System.out.println("=");	
			}
		});
		IOUtils.copyBytes(inputStream, outputStream, 50,true);
		inputStream.close();
		outputStream.flush();
		outputStream.close();
		fs.close();
	}

}
