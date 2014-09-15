/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.workflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MostSeenStartLetterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

}
