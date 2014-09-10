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

import java.io.IOException;
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
public class ZookeeperServerManager implements InitializingBean, DisposableBean
{
  /**
   * Logger for class {@link ZookeeperServerManager}.
   */
  private final static Logger log = LoggerFactory.getLogger(ZookeeperServerManager.class);
  
  private EmbeddedZookeeper      zk;

  private ThreadPoolTaskExecutor taskExecutor;

  private String directory;
  private String port;
  
  public ZookeeperServerManager() throws IOException
  {
    Properties zkProp = ZookeeperUtils.loadServerProperties();
    this.port = zkProp.getProperty("clientPort");
    this.directory = zkProp.getProperty("dataDir");
  }
  
  /**
   * @param taskExecutor the taskExecutor to set
   */
  public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor)
  {
    this.taskExecutor = taskExecutor;
  }
  
  /**
   * @param port the port to set
   */
  public void setPort(String port)
  {
    this.port = port;
  }
  
  /**
   * @param directory the directory to set
   */
  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  /* (non-Javadoc)
   * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet() throws Exception
  {
    startServer();
  }

  /* (non-Javadoc)
   * @see org.springframework.beans.factory.DisposableBean#destroy()
   */
  @Override
  public void destroy() throws Exception
  {
    zk.stop();
  }

  private void startServer()
  {
    this.zk = new EmbeddedZookeeper(port, directory);
    taskExecutor.execute(new Runnable() {
      @Override
      public void run()
      {
        log.info("starting zookeeper");
        try
        {
          zk.start();
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
  }

}
