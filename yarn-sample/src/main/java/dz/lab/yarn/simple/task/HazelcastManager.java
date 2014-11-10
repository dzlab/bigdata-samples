package dz.lab.yarn.simple.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;


public class HazelcastManager
{
  private static final Logger LOG = Logger.getLogger(HazelcastManager.class.getName());
  
  private HazelcastInstance hz;

  public void startHazelcast()
  {
    LOG.info("Starting hazelcast instance");   
    Config config = new ClasspathXmlConfig("hazelcast.xml");
    hz = Hazelcast.newHazelcastInstance(config);
  }
  
  public void stopHazelcast()
  {
    LOG.info("Stopping hazelcast instance"); 
    if(hz != null)
    {
      hz.shutdown();
    }
  }
    
  public int process(String filename) { 
    // read the file content
    Scanner s = Utils.getScanner(filename);
    if(s == null) 
      return -1;
      
    // submit tasks to hazelcast executor
    IExecutorService executor = hz.getExecutorService("executor");
    IMap<String, Integer> words = hz.getMap("words");
    List<Future<Integer>> results = new ArrayList<Future<Integer>>();
    while(s.hasNextLine()) {
      Utils.sleep(10);
      String line = s.nextLine();
      Future<Integer> r = executor.submit(new CountTask(words, line));
      results.add(r);
    }
    if(s != null) s.close();
    int total = 0;
    for(Future<Integer> r: results) {
      try {
        total += r.get();
      }catch(Exception e) {
        e.printStackTrace();
      }
    }
    LOG.info("Successfully processed " + total + " word(s).");
    return total;
  }
  
}
