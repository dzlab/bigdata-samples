/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.mapred.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BlogWritable implements WritableComparable<BlogWritable>
{
  
  private String author;
  private String content;
  
  public BlogWritable()
  {
  }
  
  public BlogWritable(String author, String content)
  {
    this.author = author;
    this.content = content;        
  }
  
  /**
   * @return the author
   */
  public String getAuthor()
  {
    return author;
  }
  /**
   * @return the content
   */
  public String getContent()
  {
    return content;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    // how data is write
    out.writeUTF(author);
    out.writeUTF(content);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    // how data is read
    author = in.readUTF();
    content = in.readUTF();
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(BlogWritable other)
  {
    // how to order
    return author.compareTo(other.author);
  }

}
