package dz.lab.hdfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Read the content of a file stored on HDFS
 */
public class ReadFile
{
  public static void main(String[] args) throws IOException
  {
    Path fileToRead = new Path("/tmp/quotes.csv");

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.2.72:8020");

    FileSystem fs = FileSystem.get(conf);

    InputStream input = null;
    try
    {
      input = fs.open(fileToRead);
      IOUtils.copyBytes(input, System.out, 4096);
    }
    finally
    {
      IOUtils.closeStream(input);
    }
  }
}
