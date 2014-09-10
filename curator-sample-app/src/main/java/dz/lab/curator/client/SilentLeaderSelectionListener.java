/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.curator.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author dzlab (dzlabs@outlook.com) , 20 août 2014
 */
public class SilentLeaderSelectionListener implements Closeable, LeaderSelectorListener
{
  /**
   * Logger for class {@link SilentLeaderSelectionListener}.
   */
  
  private final static Logger log = LoggerFactory.getLogger(SilentLeaderSelectionListener.class);

  private final String name;
  private final LeaderSelector leaderSelector;
  private final AtomicInteger leaderCount = new AtomicInteger();
  
  private volatile Thread     ourThread = null;
  
  public SilentLeaderSelectionListener(CuratorFramework client, String path, String name)
  {
      this.name = name;

      // create a leader selector using the given path for management
      // all participants in a given leader selection must use the same path
      // LeadershipListener here is also a LeaderSelectorListener but this isn't required
      leaderSelector = new LeaderSelector(client, path, this);

      // for most cases you will want your instance to requeue when it relinquishes leadership
      leaderSelector.autoRequeue();
  }
  
  public void start() throws IOException
  {
      // the selection for this instance doesn't start until the leader selector is started
      // leader selection is done in the background so this call to leaderSelector.start() returns immediately
      leaderSelector.start();
      log.info("----------------------------------------------------------------");
      log.info("starting leadership listener with name: "+name);
      log.info("----------------------------------------------------------------");
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException
  {
    leaderSelector.close();
  }
  
  /* (non-Javadoc)
   * @see org.apache.curator.framework.state.ConnectionStateListener#stateChanged(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.state.ConnectionState)
   */
  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState)
  {
    // you MUST handle connection state changes. This WILL happen in production code.
    
    if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
    {
        if ( ourThread != null )
        {
            ourThread.interrupt();
        }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.curator.framework.recipes.leader.LeaderSelectorListener#takeLeadership(org.apache.curator.framework.CuratorFramework)
   */
  @Override
  public void takeLeadership(CuratorFramework client) throws Exception
  {
    // we are now the leader. This method should not return until we want to relinquish leadership
    
    final int         waitSeconds = (int)(5 * Math.random()) + 1;

    ourThread = Thread.currentThread();
    log.info("----------------------------------------------------------------");
    log.info(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
    log.info(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
    log.info("----------------------------------------------------------------");
    try
    {
        Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
    }
    catch ( InterruptedException e )
    {
        log.warn(name + " was interrupted.");
        Thread.currentThread().interrupt();
    }
    finally
    {
        ourThread = null;
        log.debug(name + " relinquishing leadership.\n");
    }
  }

}
