package dz.lab.yarn.simple.task;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 10 nov. 2014
 */
public class Utils
{
  private static final Logger LOG = Logger.getLogger(Utils.class.getName());
  
  public static Scanner getScanner(String filename)
  {
    Scanner s = null;
    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      String pathString = "hdfs://" + hostname + filename;
      LOG.info("Reading file from: " + pathString);
      Path path = new Path(pathString);
      Configuration config = new Configuration();   
      config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      FileSystem fs = path.getFileSystem(config);
      s = new Scanner(fs.open(path));
    }catch(IOException e) {
      e.printStackTrace();
    }
    return s;
  }

  public static void sleep(long millis) {
    try
    {
      Thread.sleep(millis);
    }
    catch (InterruptedException exception)
    {
      exception.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Config config = new ClasspathXmlConfig("hazelcast.xml");    
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
    IMap<String, Integer> words = hz.getMap("words");
    StringBuilder buf = new StringBuilder(1024 * 1024);
    for(String w: words.keySet()) {
      buf.append(w).append(':').append(words.get(w)).append('\n');
    }
    hz.shutdown();
    LOG.info("result: \n"+buf.toString());
  }
}
