package dz.lab.yarn.simple.handler;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler
{
  private static final Logger LOG = Logger.getLogger(NMCallbackHandler.class.getName());
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStarted(org.apache.hadoop.yarn.api.records.ContainerId, java.util.Map)
   */
  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse)
  {
    LOG.info("onContainerStarted("+containerId+", "+allServiceResponse+") called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStatusReceived(org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus)
   */
  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus)
  {
    LOG.info("onContainerStatusReceived("+containerId+", "+containerStatus+") called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onContainerStopped(org.apache.hadoop.yarn.api.records.ContainerId)
   */
  @Override
  public void onContainerStopped(ContainerId containerId)
  {
    LOG.info("onContainerStopped("+containerId+") called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onStartContainerError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t)
  {
    LOG.info("onStartContainerError("+containerId+", "+t.getMessage()+") called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onGetContainerStatusError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t)
  {
    LOG.info("onGetContainerStatusError("+containerId+", "+t.getMessage()+") called");
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler#onStopContainerError(org.apache.hadoop.yarn.api.records.ContainerId, java.lang.Throwable)
   */
  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t)
  {
    LOG.info("onStopContainerError("+containerId+", "+t.getMessage()+") called");
  }

}
