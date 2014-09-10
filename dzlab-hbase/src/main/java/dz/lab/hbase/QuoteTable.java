/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 2 sept. 2014
 */
public class QuoteTable
{
  public static final String TABLE_NAME          = "QUOTES";
  public static final byte[] FAMILY_NAME_BYTES   = Bytes.toBytes("0");
  public static final byte[] TABLE_QUOTES_BYTES  = Bytes.toBytes(TABLE_NAME);
  public static final byte[] COLUMN_NAME_BYTES   = Bytes.toBytes("NAME");
  public static final byte[] COLUMN_SYMBOL_BYTES = Bytes.toBytes("SYMBOL");
  public static final byte[] COLUMN_PRICE_BYTES  = Bytes.toBytes("PRICE");
  
  
  
}
