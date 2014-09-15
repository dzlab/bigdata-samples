/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.hbase.custom_output;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StartsWithCountJob_HBase extends Configured implements Tool
{
  protected final static String TABLE_NAME    = "HBaseSamples";
  protected final static String FAMILY        = "count";
  protected final static String INPUT_COLUMN  = "word";

  // input and output resides under same family (doesn't have too)
  protected final static String RESULT_COLUMN = "result";

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception
  {
    Job job = Job.getInstance(getConf(), "StartsWithCount-HBase");
    job.setJarByClass(getClass());

    Scan scan = new Scan();
    scan.addColumn(toBytes(FAMILY), toBytes(INPUT_COLUMN));
    // set up job with hbase utils
    TableMapReduceUtil.initTableMapperJob(
        TABLE_NAME, 
        scan, 
        StartsWithCountMapper_HBase.class, 
        Text.class, 
        IntWritable.class, 
        job);
    TableMapReduceUtil.initTableReducerJob(
        TABLE_NAME, 
        StartsWithCountReducer_HBase.class, 
        job);
    
    job.setNumReduceTasks(1);    
    
    return job.waitForCompletion(true) ? 0: 1;
  }

  public static void main(String[] args) throws Exception
  {
    int code = ToolRunner.run(new StartsWithCountJob_HBase(), args);
    System.exit(code);
  }
}
