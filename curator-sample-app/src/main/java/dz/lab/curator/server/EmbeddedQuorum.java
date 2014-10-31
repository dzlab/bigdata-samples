package dz.lab.curator.server;

import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author dzlab (dzlabs@outlook.com) , 19 ao�t 2014
 */
public class EmbeddedQuorum
{
  /**
   * Logger for class {@link EmbeddedQuorum}.
   */
  private final static Logger log = LoggerFactory.getLogger(EmbeddedQuorum.class);

  private QuorumPeerMain quorum;
  private QuorumPeer peer;

  public EmbeddedQuorum()
  {
    this.quorum = new QuorumPeerMain();
    this.peer = getQuorumPeer();
  }

  public void start(Properties zkProp) throws Exception
  {
    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(zkProp);

    // Start and schedule the the purge task
    DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
            .getDataDir(), config.getDataLogDir(), config
            .getSnapRetainCount(), config.getPurgeInterval());
    purgeMgr.start();

    quorum.runFromConfig(config);
  }

  public void stop()
  {
    peer.shutdown();
  }

  private QuorumPeer getQuorumPeer()
  {
    QuorumPeer peer = null;
    try
    {
      Field field = QuorumPeerMain.class.getDeclaredField("quorumPeer");
      field.setAccessible(true);
      peer = (QuorumPeer) field.get(quorum);
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
    return peer;
  }
}
