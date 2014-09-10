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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class LsWithPathFilter
{

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException
  {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    Path path = new Path("/");
    // restrict result of listStatus() by supplying PathFilter    
    FileStatus[] files = fs.listStatus(path, new PathFilter(){

      @Override
      public boolean accept(Path path)
      {
        // do not show path whose name equals to user
        if(path.getName().equals("user"))
        {
          return false;
        }
        return true;
      }      
    });
    
    for(FileStatus file: files)
    {
      System.out.println(file.getPath().getName());
    }
  }

}
