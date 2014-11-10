/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.yarn.simple.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 10 nov. 2014
 */
public class CountTaskManager implements Runnable
{
  private static final Logger LOG = Logger.getLogger(CountTaskManager.class.getName());
  
  private IExecutorService executor;
  private IMap<String, Integer> words;
  
  private String filename;
  
  private boolean running;
  private boolean finished;
  
  public CountTaskManager(IExecutorService executor, IMap<String, Integer> words, String filename)
  {
    this.executor = executor;
    this.words = words;
    this.filename = filename;
    this.running = false;
    this.finished = false;
  }

  public boolean isRunning() {
    return this.running;
  }
  
  public boolean hasFinished() {
    return this.finished;
  }
  
  /* (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run()
  {
    this.running = true;
    // read the file content
    Scanner s = Utils.getScanner(filename);
    if(s == null) 
      return;
      
    // submit tasks to hazelcast executor
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
    this.running = false;
    this.finished = true;
  }

}
