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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BadRename
{

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException
  {
    FileSystem fs = FileSystem.get(new Configuration());
    Path source = new Path("/does/not/exist/file.txt");
    Path nonExistentPath = new Path("/does/not/exist/file1.txt");
    boolean result = fs.rename(source, nonExistentPath);
    System.out.println("Rename: " + result);
  }

}
