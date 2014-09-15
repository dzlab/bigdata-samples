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
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StartsWithCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  private final static IntWritable countOne = new IntWritable(1);
  private final Text reusableText = new Text();
  
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    StringTokenizer tokenizer = new StringTokenizer(value.toString());
    while(tokenizer.hasMoreTokens())
    {
      reusableText.set(tokenizer.nextToken().substring(0, 1));
      context.write(reusableText, countOne);
    }
  }
}
