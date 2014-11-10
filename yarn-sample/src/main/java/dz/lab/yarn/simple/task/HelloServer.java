package dz.lab.yarn.simple.task;

import java.util.Map;
import java.util.logging.Logger;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.ServerRunner;

/**
 * An example of subclassing NanoHTTPD to make a custom HTTP server.
 */
public class HelloServer extends NanoHTTPD
{
  private static final Logger LOG = Logger.getLogger(HelloServer.class.getName());
  
  public static void main(String[] args)
  {
    ServerRunner.run(HelloServer.class);
  }
  
  private HazelcastManager hazelcast;
  
  public HelloServer()
  {
    super(8080);
    hazelcast = new HazelcastManager();
  }

  @Override
  public Response serve(IHTTPSession session)
  {
    Method method = session.getMethod();
    String uri = session.getUri();
    Map<String, String> parms = session.getParms();
    LOG.info(method + " '" + uri + "' ");
    String msg = "";
    if(uri.startsWith("/hazelcast"))
    {
      if(uri.endsWith("start"))
      {
        // manage Hazelcast
        hazelcast.startHazelcast();
        msg = "Starting Hazelcast";
      }
      else if(uri.endsWith("stop"))
      {
        // manage Hazelcast
        hazelcast.stopHazelcast();
        msg = "Stopping Hazelcast";
      }else {
        // process the job submitted to Hazelcast
        int index = uri.indexOf("/hazelcast")+"/hazelcast".length();
        String filename = uri.substring(index);
        int lines = hazelcast.process(filename);
        msg = "Successfully processed " + lines + " word(s) from " + filename;
      }
    }
    else if(uri.startsWith("/nanohttpd"))
    {
      if(uri.endsWith("stop"))
      {
        msg = "Shutting down NanoHTTPD server";
        stopNanohttpdAsync();
      }
    }
    else
    {
      msg = printUsage();
    }
    return new NanoHTTPD.Response(msg);
  }
  
  private String printUsage()
  {
    String msg = "<html><body><h1>Hello server usage:</h1>\n";
    msg += "<p>" + "Nanohttpd  stop: /nanohttpd" + "</p>";
    msg += "<p>" + "Hazelcast start: /hazelcast/start" + "</p>";
    msg += "<p>" + "Hazelcast  stop: /hazelcast/stop" + "</p>";
    msg += "<p>" + "Hazelcast wordcount: /hazelcast/path/text/file" + "</p>";
    msg += "</body></html>\n";
    return msg;
  }
  
  private void stopNanohttpdAsync()
  {
    new Thread(new Runnable() {
      
      @Override
      public void run()
      {
        Utils.sleep(100);
        stopNanohttpd();
      }
    }).start();
  }
  
  private void stopNanohttpd()
  {
    this.stop();
    System.exit(0);
  }
}
