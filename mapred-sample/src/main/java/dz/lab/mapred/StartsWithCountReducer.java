/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StartsWithCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
  @Override
  protected void reduce(Text token, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException
  {
    int sum = 0;
    for(IntWritable count: counts)
    {
      sum += count.get();      
    }
    context.write(token, new IntWritable(sum));
  }

}
