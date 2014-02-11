package org.hadoop.io.thrift;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.hadoop.io.thrift.model.Person;

/**
 * Write a thrift object {@link Person} into hdfs
 * 
 * @author hadoop
 * Date : Feb 10, 2014
 */

public class ThriftWriterHDFS {

	/**
	 * @param args
	 * @throws TException
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void main(String[] args) throws TException, IOException,
	                URISyntaxException {
		if (args.length != 1) {
			System.out.println("Usage : hadoop jar <jar-name> org.hadoop.io.thrift.ThriftWriterHDFS <dest-path>");
			System.exit(0);
		}
		String destinationPath = args[0];
		Person person = new Person();
		person.setPersonId(1);
		person.setFirstName("FirstName");
		person.setLastName("LastName");
		person.setCity("City");
		byte[] serializedBytes = ThriftWriterHDFS.serialize(person);

		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		OutputStream outputStream = fileSystem
		                .create(new Path(destinationPath));
		outputStream.write(serializedBytes);
		outputStream.flush();
		outputStream.close();
		fileSystem.close();
	}

	/**
	 * Serialize the thrift object
	 * 
	 * @param object
	 * @return
	 * @throws TException
	 */
	private static byte[] serialize(TBase<?, ?> object) throws TException {
		TSerializer serializer = new TSerializer();
		byte[] bytes = serializer.serialize(object);
		return bytes;
	}

}
