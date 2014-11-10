package dz.lab.yarn.simple.task;

import java.util.HashMap;
import java.util.Map;
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
  private final Map<String, CountTaskManager> jobs;
  
  public HazelcastManager() {
    jobs = new HashMap<String, CountTaskManager>();
  }
  
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
    
  public boolean process(String filename) {     
    IExecutorService executor = hz.getExecutorService("executor");
    IMap<String, Integer> words = hz.getMap("words");
    CountTaskManager manager = new CountTaskManager(executor, words, filename);
    jobs.put(filename, manager);
    executor.execute(manager);   
    return true;
  }
  
  public String status(String filename) {
    CountTaskManager manager = jobs.get(filename);
    if(manager == null)
    {
      return "No job was submitted for: " + filename;
    }
    String status = "{'running': "+manager.isRunning()+", 'finished': "+manager.hasFinished()+"}";
    return status;
  }
  
}
