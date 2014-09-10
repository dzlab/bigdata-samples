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

public class CopyMove
{
  public static void copyFromLocalToHDFS(FileSystem fs) throws IOException
  {
    Path formLocal = new Path("/home/hadoop/sample.txt");
    Path toHdfs = new Path("/tmp/sample.txt");
    fs.copyFromLocalFile(formLocal, toHdfs);
  }
  
  public static void deleteData(FileSystem fs) throws IOException
  {
    Path toDelete = new Path("/tmp/sample.txt");
    boolean isDeleted = fs.delete(toDelete, false);
    System.out.println("Deleted: " + isDeleted);
  }

  public static void createDirectory(FileSystem fs) throws IOException
  {
    Path newDir = new Path("/tmp/playArea/newDir");
    boolean created = fs.mkdirs(newDir);
    System.out.println("Created: " + created);
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException
  {
    FileSystem fs = FileSystem.get(new Configuration());
  
    copyFromLocalToHDFS(fs);
    
    deleteData(fs);
  }

}
