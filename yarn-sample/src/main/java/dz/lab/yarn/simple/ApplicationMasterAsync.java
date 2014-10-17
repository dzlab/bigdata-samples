package dz.lab.yarn.simple;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import dz.lab.yarn.simple.handler.NMCallbackHandler;
import dz.lab.yarn.simple.handler.RMCallbackHandler;

public class ApplicationMasterAsync
{
  private static final Logger LOG = Logger.getLogger(ApplicationMasterAsync.class.getName());
  
  public static int completedContainers = 0;
  public static Object lock = new Object();
  
  private String appMasterHostname    = "";
  private int    appMasterRpcPort     = 0;
  private String appMasterTrackingUrl = "";

  private AMRMClientAsync<ContainerRequest> rmClient;
  
  public boolean init(String[] args)
  {
    Options opts = new Options();
    
    return true;
  }
  
  public void run(String message) throws YarnException, IOException
  {
    Configuration conf = new YarnConfiguration();

    // Initialize Node Manager client
    NMClientAsync.CallbackHandler nodeManagerListener = new NMCallbackHandler();
    NMClientAsync nmClient = NMClientAsync.createNMClientAsync(nodeManagerListener);
    nmClient.init(conf);
    nmClient.start();

    // Initialize Resource Manager client
    // AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    AMRMClientAsync.CallbackHandler allocationListener = new RMCallbackHandler(nmClient, message);
    rmClient = AMRMClientAsync.createAMRMClientAsync(100, allocationListener);
    rmClient.init(conf);
    rmClient.start();

    // Register with the Resource Manager
    RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
        appMasterTrackingUrl);
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    int n = 2;
    setupContainerAskForRM(n);
    
    monitorContainersCompletion(n);
    
    // Un-register with Resource Manager
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMasterHostname, appMasterTrackingUrl);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private void setupContainerAskForRM(int n)
  {
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager    
    for (int i = 0; i < n; i++)
    {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      LOG.info("Making resource request " + i);
      rmClient.addContainerRequest(containerAsk);
    }
  }
  
  /**
   * 
   * @param n the number of container to monitor their completion
   */
  private void monitorContainersCompletion(int n)
  {
    boolean wait = true;
    // Obtain allocated containers, launch and check for responses        
    while(wait)
    {
      try
      {
        LOG.info("Going to sleep while waiting containers to complete.");
        Thread.sleep(100);
      }
      catch(InterruptedException exception)
      {        
      }
      synchronized (lock)
      {
        if(completedContainers == n)
        {
          wait = false;
        }
      }      
    }
    LOG.info("All containers complemented.");
  }
  
  /**
   * 
   * @param args
   * @throws YarnException
   * @throws IOException
   */
  public static void main(String[] args) throws YarnException, IOException
  {
    String message = args[0];
    new ApplicationMasterAsync().run(message);
  }
}
