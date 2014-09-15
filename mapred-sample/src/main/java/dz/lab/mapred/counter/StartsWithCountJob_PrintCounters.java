/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dz.lab.mapred.StartsWithCountJob;
import dz.lab.mapred.StartsWithCountMapper;
import dz.lab.mapred.StartsWithCountReducer;

import org.apache.hadoop.mapreduce.Counter;

public class StartsWithCountJob_PrintCounters extends Configured implements Tool
{

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();
    // the following property will enable mapreduce to use its packaged local job runner
    //conf.set("mapreduce.framework.name", "local");
    
    Job job = Job.getInstance(conf, "StartsWithCountJob");
    job.setJarByClass(getClass());

    // configure output and input source
    TextInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(TextInputFormat.class);

    // configure mapper and reducer
    job.setMapperClass(StartsWithCountMapper.class);
    job.setCombinerClass(StartsWithCountReducer.class);
    job.setReducerClass(StartsWithCountReducer.class);

    // configure output
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    int resultCode = job.waitForCompletion(true) ? 0 : 1;
    System.out.println("Job is complete! Printing Counters:");
    Counters counters = job.getCounters();
    
    for(String groupName: counters.getGroupNames())
    {
      CounterGroup group = counters.getGroup(groupName);
      System.out.println(group.getDisplayName());
      
      for(Counter counter: group.getUnderlyingGroup())
      {
        System.out.println(" " + counter.getDisplayName() + "=" + counter.getValue());
      }
    }
    return resultCode;
  }

  public static void main(String[] args) throws Exception
  {
    // we need an actual java main that will execute the job
    String[] params = new String[]{"/tmp/quotes.csv", "/tmp/output"};
    int exitCode = ToolRunner.run(new StartsWithCountJob(), params);
    System.exit(exitCode);
  }
}
