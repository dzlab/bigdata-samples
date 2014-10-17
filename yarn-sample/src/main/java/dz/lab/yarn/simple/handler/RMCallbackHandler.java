package dz.lab.yarn.simple.handler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.Records;

import dz.lab.yarn.simple.ApplicationClient;
import dz.lab.yarn.simple.ApplicationMasterAsync;
import dz.lab.yarn.simple.task.HelloServer;

public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler
{
  private static final Logger LOG = Logger.getLogger(RMCallbackHandler.class.getName());

  private final NMClientAsync nmClient;
  private final String message;  

  public RMCallbackHandler(NMClientAsync nmClient, String message)
  {
    this.nmClient = nmClient;
    this.message = message;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onContainersCompleted(java.util.List)
   */
  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses)
  {
    LOG.info("onContainersCompleted() called");
    for (ContainerStatus status : statuses)
    {
      LOG.info("container '" + status.getContainerId() + "' status is " + status);
      synchronized (ApplicationMasterAsync.lock)
      {
        ApplicationMasterAsync.completedContainers++;
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onContainersAllocated(java.util.List)
   */
  @Override
  public void onContainersAllocated(List<Container> containers)
  {
    LOG.info("onContainersAllocated() called");
    for (Container container : containers)
    {
      LOG.info("container allocated is " + container);

      // Launch container by create ContainerLaunchContext
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

      String logDir = System.getenv(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
            
      String command = new StringBuilder("/bin/echo ").append(message)
          .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/echo_stdout")
          .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/echo_stderr")
          .toString();
      
      LOG.info("Command to execute: " + command);
      List<String> commands = Arrays.asList(command);
      ctx.setCommands(commands);
      LOG.info("[AM] Launching container " + container.getId());
      nmClient.startContainerAsync(container, ctx);
    }

  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onShutdownRequest()
   */
  @Override
  public void onShutdownRequest()
  {
    LOG.info("onShutdownRequest() called");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onNodesUpdated(java.util.List)
   */
  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes)
  {
    LOG.info("onNodesUpdated() called");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#getProgress()
   */
  @Override
  public float getProgress()
  {
    LOG.info("getProgress() called");
    return 0;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler#onError(java.lang.Throwable)
   */
  @Override
  public void onError(Throwable e)
  {
    LOG.info("onError() called: " + e);
    e.printStackTrace();
  }

}
