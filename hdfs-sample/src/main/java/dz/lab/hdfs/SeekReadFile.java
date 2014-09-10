/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class SeekReadFile
{

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException
  {
    Path fileToRead = new Path("/tmp/quotes.csv");
    // read configuration from core-site.xml available in the classpath (under /resources)
    FileSystem fs = FileSystem.get(new Configuration());
    
    FSDataInputStream input = null;
    try
    {
      // start at position 0
      input = fs.open(fileToRead);
      System.out.print("start position=" + input.getPos() + ":");
      IOUtils.copyBytes(input, System.out, 4096, false);
      
      // seek to position 11
      input.seek(11);
      System.out.print("start position=" + input.getPos() + ":");
      IOUtils.copyBytes(input, System.out, 4096, false);
      
      // seek back to position 0
      input.seek(11);
      System.out.print("start position=" + input.getPos() + ":");
      IOUtils.copyBytes(input, System.out, 4096, false);
    }
    finally
    {
      IOUtils.closeStream(input);
    }
  }

}
