package dz.lab.curator.server;

import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;

import javax.management.JMException;

import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author dzlab (dzlabs@outlook.com) , 19 ao�t 2014
 */
public class EmbeddedZookeeper
{
  static Logger               LOG = LoggerFactory.getLogger(EmbeddedZookeeper.class);

  private String              port;
  private String              directory;
  private ZooKeeperServerMain zk;

  public EmbeddedZookeeper(String port, String directory)
  {
    LOG.info("Storing zookeeper data to {}", directory);
    this.port = (port != null ? port : "2181");
    this.directory = directory;
    this.zk = new ZooKeeperServerMain();
  }

  public void start() throws Exception
  {
    LOG.info("Running zookeeper on thread '{}' with id {}", Thread.currentThread().getName(), Thread.currentThread().getId());
    try
    {
      ManagedUtil.registerLog4jMBeans();
    }
    catch (JMException e)
    {
      LOG.warn("Unable to register log4j JMX control", e);
    }
    ServerConfig config = new ServerConfig();
    config.parse(new String[] { port, directory });
    zk.runFromConfig(config);
  }

  /**
   * @return the port
   */
  public String getPort()
  {
    return port;
  }

  /**
   * @return the directory
   */
  public String getDirectory()
  {
    return directory;
  }

  public void stop() throws Exception
  {
    Field cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
    cnxnFactoryField.setAccessible(true);
    ServerCnxnFactory cnxnFactory = (ServerCnxnFactory) cnxnFactoryField.get(this);
    cnxnFactory.closeAll();

    Field ssField = cnxnFactory.getClass().getDeclaredField("ss");
    ssField.setAccessible(true);
    ServerSocketChannel ss = (ServerSocketChannel) ssField.get(cnxnFactory);
    ss.close();
  }
}
