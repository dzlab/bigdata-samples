package dz.lab.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author dzlab (dzlabs@outlook.com) , 14 ao�t 2014
 */
@Controller
@RequestMapping("/stats")
public class ZKController
{
  private final Logger log = LoggerFactory.getLogger(getClass());

  CuratorFramework client;

  public ZKController()
  {
    // start client
    client = CuratorFrameworkFactory.builder()
        .connectString("localhost:2181")
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .namespace("heavenize")
        .build();

    client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState)
      {
        log.info("State changed to: "+newState);
      }
    });
  }

  @RequestMapping(method = RequestMethod.GET)
  public String get()
  {
    return "zookeeper connection on default port 2181 is "+client.getState();
  }
}
