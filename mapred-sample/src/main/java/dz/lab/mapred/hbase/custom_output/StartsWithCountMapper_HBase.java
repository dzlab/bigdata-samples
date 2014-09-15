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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class StartsWithCountMapper_HBase extends TableMapper<Text, IntWritable>
{
  private final static IntWritable countOne = new IntWritable(1);
  private static final String FAMILY = null;
  private static final String COLUMN = null;
  private final Text reusableText = new Text();
  
  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException
  {
    // retrieve value from input column
    byte[] bytes = value.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN));
    
    String str = Bytes.toString(bytes);
    
    reusableText.set(str.substring(0, 1));
    context.write(reusableText, countOne);
  }
}
