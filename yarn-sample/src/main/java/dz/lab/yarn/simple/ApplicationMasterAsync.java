package dz.lab.yarn.simple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
  private static final Logger               LOG                  = Logger.getLogger(ApplicationMasterAsync.class.getName());

  public static int                         completedContainers  = 0;
  public static Object                      lock                 = new Object();

  private String                            appMasterHostname    = "";
  private int                               appMasterRpcPort     = 0;
  private String                            appMasterTrackingUrl = "";
  private String                            jarPath;
  private int                               numContainers        = 2;
  private AMRMClientAsync<ContainerRequest> rmClient;

  /**
   * @param args
   * @throws YarnException
   * @throws IOException
   * @throws URISyntaxException
   */
  public static void main(String[] args)
  {
    ApplicationMasterAsync appMaster = new ApplicationMasterAsync();
    try
    {
      if (!appMaster.init(args))
      {
        System.exit(1);
      }
      LOG.info("Running the application master!");
      appMaster.run();
    }
    catch (Exception exception)
    {
      LOG.info("Failed tp run application master!");
      exception.printStackTrace();
    }

  }

  public boolean init(String[] args) throws ParseException, URISyntaxException
  {
    // prepare options parser
    Options opts = new Options();
    opts.addOption("jar", true, "JAR file containing the application");
    opts.addOption("help", false, "Print usage");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("num_containers", true, "Number of containers to launch");
    // parse given CLI arguments
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0)
    {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for application master to initialize.");
    }
    if (cliParser.hasOption("help"))
    {
      printUsage(opts);
      return false;
    }
    if (!cliParser.hasOption("jar"))
    {
      LOG.info("Missing path to jar file!");
      printUsage(opts);
      return false;
    }
    this.jarPath = new URI(cliParser.getOptionValue("jar")).getPath();
    if (cliParser.hasOption("num_containers"))
    {
      this.numContainers = Integer.valueOf(cliParser.getOptionValue("num_containers"));
    }
    LOG.info("Jar path is " + jarPath);
    return true;
  }

  public void run() throws YarnException, IOException
  {
    Configuration conf = new YarnConfiguration();

    // Initialize Node Manager client
    LOG.info("Initializing the node manager client");
    NMClientAsync.CallbackHandler nodeManagerListener = new NMCallbackHandler();
    NMClientAsync nmClient = NMClientAsync.createNMClientAsync(nodeManagerListener);
    nmClient.init(conf);
    nmClient.start();

    // Initialize Resource Manager client
    LOG.info("Initializing the resource manager client");
    // AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    AMRMClientAsync.CallbackHandler allocationListener = new RMCallbackHandler(nmClient, this.jarPath);
    rmClient = AMRMClientAsync.createAMRMClientAsync(100, allocationListener);
    rmClient.init(conf);
    rmClient.start();

    // Register with the Resource Manager
    LOG.info("Registering the application master");
    RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
        appMasterTrackingUrl);
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    setupContainerAskForRM();

    monitorContainersCompletion();

    // Un-register with Resource Manager
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMasterHostname, appMasterTrackingUrl);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private void setupContainerAskForRM()
  {
    LOG.info("Setting up the containers for this application master");
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager
    for (int i = 0; i < this.numContainers; i++)
    {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      LOG.info("Making resource request " + i);
      rmClient.addContainerRequest(containerAsk);
    }
  }

  /**
   * @param n the number of container to monitor their completion
   */
  private void monitorContainersCompletion()
  {
    LOG.info("Monitoring the completion of containers of this application master");
    boolean wait = true;
    // Obtain allocated containers, launch and check for responses
    while (wait)
    {
      try
      {
        LOG.info("Going to sleep while waiting containers to complete.");
        Thread.sleep(100);
      }
      catch (InterruptedException exception)
      {
      }
      synchronized (lock)
      {
        if (completedContainers == this.numContainers)
        {
          wait = false;
        }
      }
    }
    LOG.info("All containers complemented.");
  }

  /**
   * Helper function to print usage
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts)
  {
    new HelpFormatter().printHelp("ApplicationMasterAsync", opts);
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo()
  {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet())
    {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr = null;
    try
    {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line = "";
      while ((line = buf.readLine()) != null)
      {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
      buf.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }
}
