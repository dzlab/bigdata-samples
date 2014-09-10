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
import org.apache.hadoop.fs.Path;

public class LoadConfiguration
{
  private final static String PROP_NAME = "fs.default.name";
  
  static class Vars
  {
    final static String HADOOP_HOME = "/usr/local/hadoop";
  }
  
  public static void main(String[] args)
  {
    Configuration conf = new Configuration();
    
    // Print the property with empty configuration
    System.out.println("After construction: "+conf.get(PROP_NAME));
    
    // Add properties from core-site.xml
    conf.addResource(new Path(Vars.HADOOP_HOME + "/conf/core-site.xml"));
    System.out.println("After addResource: "+conf.get(PROP_NAME));
    
    // Manually set the property
    conf.set(PROP_NAME, "hdfs://192.168.2.72:8020/");
    System.out.println("After set: "+conf.get(PROP_NAME));
  }

}
