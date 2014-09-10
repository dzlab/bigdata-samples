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
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SimpleGlobbing
{

  public static void main(String[] args) throws IOException
  {
    Scanner in = new Scanner(System.in);
    System.out.print("Type in a glob (e.g. '/tmp/glob'): ");
    
    // read glob from command line    
    Path glob = new Path(in.next());
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] files = fs.globStatus(glob);

    // similar usage to listStatus method
    for(FileStatus file: files)
    {
      System.out.println(file.getPath().getName());
    }
  }
}
