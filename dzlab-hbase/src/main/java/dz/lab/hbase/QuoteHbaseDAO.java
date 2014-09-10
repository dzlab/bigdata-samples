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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class QuoteHbaseDAO implements QuoteDAO
{

  private final HConnection     connection;

  /**
   * A factory method for creating {@link QuoteHbaseDAO} instances
   * @param tableName the name of the HBase table
   * @return the created {@link QuoteHbaseDAO} or <code>null</code> if an exception occurred.
   */
  public QuoteHbaseDAO() throws Exception
  {
    Configuration config = HBaseConfiguration.create();

    this.connection = HConnectionManager.createConnection(config);
  }

  @Override
  public void createTable() throws Exception
  {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(QuoteTable.TABLE_NAME);
    // create a column family
    //HColumnDescriptor c = new HColumnDescriptor(COLUMN_NAME_BYTES);
    //c.setMaxVersions(1);
    desc.addFamily(new HColumnDescriptor("0"));
    //desc.addFamily(new HColumnDescriptor(COLUMN_SYMBOL_BYTES));
    //desc.addFamily(new HColumnDescriptor(COLUMN_PRICE_BYTES));
    // perform creation
    admin.createTable(desc);
    admin.close();
  }
  
  @Override
  public Quote findBySymbol(String key) throws IOException
  {
    Get g = new Get(Bytes.toBytes(key));
    g.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_NAME_BYTES);
    g.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_PRICE_BYTES);
    g.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_SYMBOL_BYTES);

    HTableInterface table = connection.getTable(QuoteTable.TABLE_QUOTES_BYTES);
    Result result = table.get(g);
    return parse(result);
  }

  @Override
  public void put(Quote q) throws IOException
  {
    Put p = new Put(Bytes.toBytes(q.symbol));
    p.add(QuoteTable.FAMILY_NAME_BYTES, Bytes.toBytes("NAME"), Bytes.toBytes(q.name));
    p.add(QuoteTable.FAMILY_NAME_BYTES, Bytes.toBytes("SYMBOL"), Bytes.toBytes(q.symbol));
    p.add(QuoteTable.FAMILY_NAME_BYTES, Bytes.toBytes("PRICE"), Bytes.toBytes(q.price));

    HTableInterface table = connection.getTable(QuoteTable.TABLE_QUOTES_BYTES);
    table.put(p);
  }

  /**
   * Parse a {@link Quote} from information retrieved from an hbase query result
   * @param result the {@link Result} instance as returned from an hbase query
   * @return the created {@link Quote}
   */
  public static Quote parse(Result result)
  {
    Quote quote;
    byte[] b = result.getValue(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_NAME_BYTES);
    String name = Bytes.toString(b);
    b = result.getValue(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_PRICE_BYTES);
    Double price = Bytes.toDouble(b);
    b = result.getValue(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_SYMBOL_BYTES);
    String symbol = Bytes.toString(b);
    quote = new Quote(name, symbol, price);
    return quote;
  }
  
  @Override
  public Iterable<Quote> get() throws IOException
  {
    List<Quote> quotes = new ArrayList<Quote>();
    Scan s = new Scan();
    // s.setCaching(10);
    s.addFamily(QuoteTable.FAMILY_NAME_BYTES);
    s.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_NAME_BYTES);
    s.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_PRICE_BYTES);
    s.addColumn(QuoteTable.FAMILY_NAME_BYTES, QuoteTable.COLUMN_SYMBOL_BYTES);

    HTableInterface table = connection.getTable(QuoteTable.TABLE_QUOTES_BYTES);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner)
    {
      System.out.println("Found row: " + result);
      Quote quote = parse(result);
      quotes.add(quote);
    }
    if (scanner != null)
    {
      scanner.close();
    }
    return quotes;
  }

  /*
   * (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException
  {
    connection.close();
  }

}
