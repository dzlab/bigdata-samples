package dz.lab.yarn.simple;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Hello world!
 */
public class ApplicationClient
{
  private static final Logger LOG = Logger.getLogger(ApplicationClient.class.getName());
  
  private YarnConfiguration yarnConf;
  private static String appName = "yarn-sample";  
  private Path srcJarPath;
  private Path dstJarPath;

  public static void main(String[] args) throws YarnException, IOException, InterruptedException, ParseException
  {
    ApplicationClient appClient = new ApplicationClient();
    if(!appClient.init(args))
    {
      System.exit(1);
    }
    appClient.run();
  }
  
  
  public ApplicationClient()
  {
    yarnConf = new YarnConfiguration();
  }
  
  /**
   * The command that will be executed by the application master
   * @throws ParseException 
   */  
  public boolean init(String[] args) throws ParseException
  {
    // prepare options parser
    Options opts = new Options();
    opts.addOption("jar", true, "JAR file containing the application");
    opts.addOption("help", false, "Print usage");
    opts.addOption("debug", false, "Dump out debug information");
    
    // parse given CLI arguments
    CommandLine cliParser = new GnuParser().parse(opts, args);
    
    if (!cliParser.hasOption("jar")) {
      LOG.info("No jar file is specified for the application master");
      return false;
    }
    
    String jar = cliParser.getOptionValue("jar");
    this.srcJarPath = new Path(jar);
    return true;
  }
  
  public void run() throws YarnException, IOException
  {
    // Create a Yarn Client    
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
    yarnClient.start();
    
    // Use YarnClient to create an application
    YarnClientApplication app = yarnClient.createApplication();
    
    checkMaximumResources(app);
    
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();    
    ApplicationId appId = appContext.getApplicationId();
    
    String sampleAppUri = copyAppJarToHDFS(appId);
    
    // Set up the launch context for the container that will host the application master
    StringBuilder commandBuilder = new StringBuilder("$JAVA_HOME/bin/java");
    //commandBuilder.append(" -classpath ").append(sampleAppUri);
    commandBuilder.append(" ").append(ApplicationMasterAsync.class.getName());
    commandBuilder.append(" -jar ").append(sampleAppUri);
    //commandBuilder.append(" ").append(message);
    commandBuilder.append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout");
    commandBuilder.append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr");
    LOG.info("Command for invoking application master is: "+commandBuilder);
    List<String> commands = Collections.singletonList(commandBuilder.toString());
    
    ContainerLaunchContext containerCtxt = Records.newRecord(ContainerLaunchContext.class);
    containerCtxt.setCommands(commands);
    
    // Setup a jar for ApplicationMaster
    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    FileStatus jarStat = FileSystem.get(yarnConf).getFileStatus(dstJarPath);
    LOG.info("AppMaster jar file status is "+jarStat.toString());
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(dstJarPath));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    containerCtxt.setLocalResources(Collections.singletonMap("yarn-sample.jar", appMasterJar));
    
    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    for(String s: yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH))
    {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), s.trim());
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + File.separator + "*");
    containerCtxt.setEnvironment(appMasterEnv);
    LOG.info("AppMaster classpath: "+appMasterEnv.toString());
    
    // Setup CPU and memory resources required by ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);
    
    // Setup the submission context of the application
    appContext.setApplicationName(appName);
    appContext.setAMContainerSpec(containerCtxt);
    appContext.setResource(capability);
    appContext.setQueue("default");
    
    // submit the application
    LOG.info("Submitting application with id: " + appId);
    yarnClient.submitApplication(appContext);
    
    // Monitor the application state
    monitorApplication(yarnClient, appId);    
  }
  
  public void checkMaximumResources(YarnClientApplication app)
  {
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("maximum available memory for ApplicationMaster is: " + maxMem);
    int maxVC = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("maximum available virtual cores for ApplicationMaster is: " + maxVC);
  }  
  /**
   * 
   * @param appId
   * @return a string URI to the destination jar file
   * @throws IOException
   */
  public String copyAppJarToHDFS(ApplicationId appId) throws IOException
  {
    FileSystem fs = FileSystem.get(yarnConf);
    String pathSuffix = appName + File.separator + appId.getId() + File.separator + "yarn-sample.jar";
    this.dstJarPath = new Path(fs.getHomeDirectory(), pathSuffix);
    fs.copyFromLocalFile(false, true, this.srcJarPath, this.dstJarPath);
    String sampleAppUri = dstJarPath.toUri().toString();
    LOG.info("Copied jar file to hdfs: " + sampleAppUri);
    return sampleAppUri;
  }
  
  public static void setupAppMasterJar()
  {
    //TODO grap code here from main()
  }
  
  /**
   * Monitor the submitted application for completion. Kill application if
   * time expires.
   * 
   * @param appId
   *            Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private boolean monitorApplication(YarnClient yarnClient, ApplicationId appId)
      throws YarnException, IOException {

    while (true) {

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.finest("Thread sleep in monitoring loop interrupted");
      }

      ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", clientToAMToken="
          + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost="
          + report.getHost() + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState="
          + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus jbossStatus = report
          .getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == jbossStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString()
              + ", JBASFinalStatus=" + jbossStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish." + " YarnState="
            + state.toString() + ", JBASFinalStatus="
            + jbossStatus.toString() + ". Breaking monitoring loop");
        return false;
      }
    }
  }
}
