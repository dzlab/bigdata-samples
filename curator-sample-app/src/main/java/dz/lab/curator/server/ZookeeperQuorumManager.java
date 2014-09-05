/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.curator.server;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import dz.lab.curator.commons.ZookeeperUtils;

/**
 * 
 * @author dzlab (dzlabs@outlook.com) , 21 août 2014
 */
public class ZookeeperQuorumManager implements InitializingBean, DisposableBean
{
  /**
   * Logger for class {@link ZookeeperQuorumManager}.
   */
  private final static Logger log = LoggerFactory.getLogger(ZookeeperQuorumManager.class);
  
  private final EmbeddedQuorum         quorum;

  private ThreadPoolTaskExecutor taskExecutor;

  public ZookeeperQuorumManager()
  {
    this.quorum = new EmbeddedQuorum();
  }
  
  /**
   * @param taskExecutor the taskExecutor to set
   */
  public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor)
  {
    this.taskExecutor = taskExecutor;
  }

  /* (non-Javadoc)
   * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet() throws Exception
  {
    //ZKUtils.configure();
    startQuorum();
  }

  /* (non-Javadoc)
   * @see org.springframework.beans.factory.DisposableBean#destroy()
   */
  @Override
  public void destroy() throws Exception
  {
    quorum.stop();
  }

  private void startQuorum()
  {
    taskExecutor.execute(new Runnable() {
      @Override
      public void run()
      {
        log.info("starting zookeeper");
        try
        {
          Properties zkProp = ZookeeperUtils.loadServerProperties();
          quorum.start(zkProp);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
  }
}
