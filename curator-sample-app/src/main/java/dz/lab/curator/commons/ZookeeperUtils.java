/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.curator.commons;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dz.lab.curator.client.ZookeeperClientManager;

/**
 * 
 * @author dzlab (dzlabs@outlook.com) , 21 août 2014
 */
public class ZookeeperUtils
{
  /**
   * Logger for class {@link ZookeeperUtils}.
   */  
  private final static Logger log = LoggerFactory.getLogger(ZookeeperUtils.class);


  public static String           PORT      = String.valueOf((int) (Math.random() * 50000));
  public static String           DIRECTORY = new File(System.getProperty("java.io.tmpdir"), "zookeeper/" + PORT)
                                               .getAbsolutePath();
  public static int              MYID      = (int) (Math.random() * 4) + 1;

  
  public static Properties loadServerProperties() throws IOException
  {
    InputStream in = ZookeeperUtils.class.getResourceAsStream("/zoo.cfg");
    Properties properties = new Properties();    
    properties.load(in);
    
    log.info("starting zk quorum with properties: {}", properties);
        
    return properties;
  }
  
  public static Properties createProperties()
  {
    Properties properties = new Properties();
    properties.setProperty("clientPort", PORT);
    properties.setProperty("dataDir", DIRECTORY);
    
    File logDir = new File(DIRECTORY+"\\log");
    logDir.mkdirs();
    properties.setProperty("dataLogDir", logDir.getAbsolutePath());  
    
    properties.setProperty("server."+MYID, "localhost:"+(2887+MYID)+":"+(3887+MYID));
    
    return properties;
  }
  


  public static void configure() throws IOException
  {
    // set the 'myid' file
    File directory = new File(DIRECTORY);
    directory.mkdirs();
    File myid = new File(directory, "myid");
    FileWriter writer = new FileWriter(myid);
    BufferedWriter buf = new BufferedWriter(writer);
    buf.write(String.valueOf(MYID));
    buf.close();
    writer.close();
  }
}
