package dz.lab.yarn.simple.task;

import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.hazelcast.core.IMap;

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 10 nov. 2014
 */
public class CountTask implements Callable<Integer>
{
  private static final Logger LOG = Logger.getLogger(CountTask.class.getName());
  
  private String line;
  private IMap<String, Integer> words;
  
  public CountTask(IMap<String, Integer> words, String line) {
    this.words = words;
    this.line = line;
  }
  

  /* (non-Javadoc)
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Integer call() throws Exception
  {
    String hostname = InetAddress.getLocalHost().getHostName();
    LOG.info("Counting words from host: " + hostname);
    int total = 0;
    String[] ws = line.toLowerCase().split("\\s+");
    for(String w: ws) {      
      Integer count = words.get(w);
      if(count == null) {
        count = Integer.valueOf(1);
      }else {
        count++;
      }
      words.put(w, count);
      total++;
    }
    return Integer.valueOf(total);
  }

}
