package dz.lab.mapred.workflow;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dz.lab.mapred.StartsWithCountJob;
import dz.lab.mapred.StartsWithCountMapper;
import dz.lab.mapred.StartsWithCountReducer;

/**
 * Hello world!
 */
public class MostSeenStartLetterJobControl extends Configured implements Tool
{
  private final Logger log = Logger.getLogger(getClass());

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception
  {
    String inputText = args[0];
    String finalOutput = args[1];
    
    // manage intermediate output directory
    String intermediateTempDir = "/tmp/" + getClass().getSimpleName() + "-tmp";
    Path intermediatePath = new Path(intermediateTempDir);
    //deleteIntermediateDir(intermediatePath);

    // JobControl manages workflow
    JobControl control = new JobControl("Workflow-Example");
    // returns Job object with property configured job
    ControlledJob step1 = new ControlledJob(getCountJob(inputText, intermediateTempDir), null);
    // specify dependencies, in this case step2 depends on step1
    ControlledJob step2 = new ControlledJob(getMostSeenJob(intermediateTempDir, finalOutput), Arrays.asList(step1));
    
    //3. add each ControlledJob to JobControl
    control.addJob(step1);
    control.addJob(step2);
    
    //4. execute JobControl in a Thread
    Thread workflowThread = new Thread(control, "Workflow-Thread");
    
    workflowThread.setDaemon(true);
    workflowThread.start();

    // simply wait for all jobs to complete
    while(!control.allFinished())
    {
      Thread.sleep(500);
    }
    
    // report results or display errors
    if(control.getFailedJobList().size() > 0)
    {
      log.error(control.getFailedJobList().size() + " jobs failed!");
      for(ControlledJob job: control.getFailedJobList())
      {
        log.error(job.getJobName() + " failed");
      }
    }
    else
    {
      log.info("Success!! Workflow completed [" + control.getSuccessfulJobList().size() + "] jobs");
    }
    return 0;
  }
  
  // build each job the same way we've done before
  private Job getCountJob(String inputText, String tempOutputPath) throws IOException
  {
    Job job = Job.getInstance(getConf(), "StartsWithCount");
    job.setJarByClass(getClass());
    
    // configure output and input source
    TextInputFormat.addInputPath(job, new Path(inputText));
    job.setInputFormatClass(TextInputFormat.class);
    
    // configure mapper and reducer
    job.setMapperClass(StartsWithCountMapper.class);
    job.setCombinerClass(StartsWithCountReducer.class);
    job.setReducerClass(StartsWithCountReducer.class);
    
    // configure output
    TextOutputFormat.setOutputPath(job, new Path(tempOutputPath));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job;
  }
  
  private Job getMostSeenJob(String intermediateTempDir, String finalOutput) throws IOException
  {
    Job job = Job.getInstance(getConf(), "MostSeenStartLetter");
    job.setJarByClass(getClass());
    
    // configure output and input source
    KeyValueTextInputFormat.addInputPath(job, new Path(intermediateTempDir));
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    
    // configure mapper and reducer
    job.setMapperClass(MostSeenStartLetterMapper.class);
    job.setCombinerClass(MostSeenStartLetterReducer.class);
    job.setReducerClass(MostSeenStartLetterReducer.class);
    
    // configure output
    TextOutputFormat.setOutputPath(job, new Path(finalOutput));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new StartsWithCountJob(), args);
    System.exit(exitCode);
  }

}
