/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.exclude;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import dz.lab.mapred.StartsWithCountMapper;
import dz.lab.mapred.StartsWithCountReducer;

/**
 * 
 * 
 */
public class StartsWithCountJob_DistCacheAPI extends Configured implements Tool
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
    
    Path toCache = new Path("/training/data/startWithExcludeFile.txt");
    // add file to cache
    job.addCacheFile(toCache.toUri());
    // create symbolic links for all files in DistributedCache; without the links you would have to use fully qualified path
    job.createSymlink();
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

}
