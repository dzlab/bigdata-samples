package dz.lab.yarn.simple.task;

import java.util.Collections;
import java.util.logging.Logger;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import dz.lab.yarn.simple.ApplicationClient;


/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch
 * the container that will execute the shell command.
 */
public class ContainerLauncherTask implements Runnable
{
  private static final Logger LOG = Logger.getLogger(ContainerLauncherTask.class.getName());
  
  Container container;
  
  public ContainerLauncherTask(NMClient nmClient, Container container)
  {
    this.container = container;
  }
  
  /**
   * Connects to CM, sets up container launch context for shell command and eventually dispatches the container start request to the CM.
   */
  @Override
  public void run()
  {
    String containerId = container.getId().toString();
    /*
     * String dateCommand = new StringBuilder("/bin/date")
     * .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
     * .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr") .toString(); String lsCommand = new
     * StringBuilder("/bin/ls") .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
     * .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr") .toString();
     */
    
    String simpleApp = new StringBuilder("$JAVA_HOME/bin/java").append(" -jar ").append(ApplicationClient.sampleAppUri)
        .append(" ").append(HelloServer.class.getName()).append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append("/stdout").append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr").toString();
    // List<String> commands = Arrays.asList(dateCommand, lsCommand, simpleApp);
    // List<String> commands = Arrays.asList(simpleApp);
    
    String command = "ls";
    LOG.info("Launching container "+containerId+" to run a command: "+command);    

    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setCommands(Collections.singletonList(command +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

  }

}
