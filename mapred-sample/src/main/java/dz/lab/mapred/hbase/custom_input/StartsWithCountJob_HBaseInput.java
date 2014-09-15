/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.hbase.custom_input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import dz.lab.mapred.StartsWithCountReducer;
import dz.lab.mapred.hbase.custom_output.StartsWithCountMapper_HBase;

public class StartsWithCountJob_HBaseInput extends Configured implements Tool
{
  protected final static String TABLE_NAME = "HBaseSamples";
  protected final static byte[] FAMILY     = toBytes("count");
  protected final static byte[] COLUMN     = toBytes("word");

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception
  {
    Job job = Job.getInstance(getConf(), "StartsWithCount-FromHBase");
    job.setJarByClass(getClass());

    // set HBase InputFormat
    job.setInputFormatClass(TableInputFormat.class);
    // new mapper to handle data from HBase
    job.setMapperClass(StartsWithCountMapper_HBase.class);

    // add hbase configuration
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    TableMapReduceUtil.addDependencyJars(job);

    // specify table and column to read from
    conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
    conf.set(TableInputFormat.SCAN_COLUMNS, "count:word");

    // configure mapper and reducer
    job.setCombinerClass(StartsWithCountReducer.class);
    job.setReducerClass(StartsWithCountReducer.class);

    // configure output
    TextOutputFormat.setOutputPath(job, new Path(args[0]));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
