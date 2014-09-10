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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class WriteToFile
{

  public static void main(String[] args) throws IOException
  {
    String textToWrite = "Hello HDFS! Elephants are awesome!\n";
    InputStream in = new BufferedInputStream(new ByteArrayInputStream(textToWrite.getBytes()));
    Path toHdfs = new Path("/tmp/writeMe.txt");
    
    Configuration conf = new Configuration();
    // create file system instance
    FileSystem fs = FileSystem.get(conf);
    // open output stream
    FSDataOutputStream out = fs.create(toHdfs);
    // copy data
    IOUtils.copyBytes(in, out, conf);
    
    // notifying client with progress
    FSDataOutputStream out2 = fs.create(toHdfs, new Progressable() {      
      @Override
      public void progress()
      {
        System.out.println("..");
      }
    });
    
    // Overwrite flag
    FSDataOutputStream out3 = fs.create(toHdfs, false);
  }
  
  
}
