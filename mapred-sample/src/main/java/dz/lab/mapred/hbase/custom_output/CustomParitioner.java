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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import dz.lab.mapred.model.BlogWritable;

/**
 * 
 * A blog partitioner that ensure that all blogs with the same author will endup in the same reduce task
 */
public class CustomParitioner extends Partitioner<Text, BlogWritable>
{

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
   */
  @Override
  public int getPartition(Text key, BlogWritable blog, int numReduceTasks)
  {
    // use the author hash AND with integer to get positive value
    int positiveHash = blog.getAuthor().hashCode()& Integer.MAX_VALUE;
    // assign a reduce task by index
    return positiveHash % numReduceTasks;    
  }

}
