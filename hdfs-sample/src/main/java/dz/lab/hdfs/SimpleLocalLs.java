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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SimpleLocalLs
{
  public static void main(String[] args) throws Exception
  {
    Path path = new Path("/");
    // read location from command line arguments
    if( args.length == 1)
    {
      path = new Path(args[0]);
    }
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.2.72:8020");
    
    FileSystem fs = FileSystem.get(conf);
    
    // list the files and direcotries under the provided path
    FileStatus[] files = fs.listStatus(path);
    for(FileStatus file: files)
    {
      System.out.println(file.getPath().getName());
    }
  }

}
