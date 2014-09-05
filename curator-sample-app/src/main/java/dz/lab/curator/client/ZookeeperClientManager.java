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

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author dzlab (dzlabs@outlook.com) , 19 août 2014
 */
public class ZookeeperClientManager implements InitializingBean, DisposableBean
{
  /**
   * Logger for class {@link ZookeeperClientManager}.
   */
  private final static Logger    log       = LoggerFactory.getLogger(ZookeeperClientManager.class);

  private CuratorFramework client;
  private SilentLeaderSelectionListener listener;

  private final String connectString;
  
  public ZookeeperClientManager() throws IOException
  {
    Properties zkProp = new Properties();
    zkProp.load(getClass().getResourceAsStream("/zk.properties"));
    
    String port = zkProp.getProperty("clientPort");
    port = port!=null ? port:"2181";
    String addr = zkProp.getProperty("serverAddr");
    addr = addr!=null ? addr: "localhost";
    
    this.connectString = String.format("%s:%s", addr, port);
  }
  
  /*
   * (non-Javadoc)
   * @see org.springframework.beans.factory.DisposableBean#destroy()
   */
  @Override
  public void destroy() throws Exception
  {
    client.close();
    listener.close();
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet() throws Exception
  {
    startClient();
    startListener();
  }

  /**
   * 
   */
  private void startClient()
  {
    // create client
    client = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .namespace("heavenize")
        .build();
    //
    client.getConnectionStateListenable().addListener(new ConnectionStateListener() {      
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState)
      {
        log.info("State changed to: "+newState);
      }
    });
    // start client
    client.start();
  }
  
  private void startListener() throws IOException
  {
    String[] animals = {"elephant", "lion", "python", "tiger", "mouse", "zebra", "fennec", "wolf", "shark", "monkey", "panda"};
    String name = animals[(int)(Math.random()*animals.length)];
    String path = "/heavenize/leader";
    listener = new SilentLeaderSelectionListener(client, path, name);
    listener.start();
  }

}
