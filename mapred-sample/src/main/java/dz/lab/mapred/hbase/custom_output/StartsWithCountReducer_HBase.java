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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * 
 * 
 */
public class StartsWithCountReducer_HBase extends TableReducer<Text, IntWritable, ImmutableBytesWritable>
{
  protected final static String FAMILY        = "count";
  protected final static String RESULT_COLUMN = "result";
  
  @Override
  protected void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException 
  {
    int sum = 0;
    for(IntWritable count: counts)
    {
      sum += count.get();
    }
    // reducer must output either Put or Delete object
    Put put = new Put(key.copyBytes());
    put.add(toBytes(FAMILY), toBytes(RESULT_COLUMN), toBytes(Integer.toString(sum)));
    context.write(null, put);    
  }

}
