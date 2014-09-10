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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 2 sept. 2014
 */
public class QuotePhoenixDAO implements QuoteDAO
{
  private final Connection conn;
  
  public QuotePhoenixDAO() throws SQLException
  {
    Properties props = new Properties();
    // Ensure the driver is on classpath
    // Connect through Phoenix to the zookeeper quorum with a host name of myServer
    this.conn = DriverManager.getConnection("jdbc:phoenix:annaba", props);
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws Exception
  {
    conn.close();
  }

  /* (non-Javadoc)
   * @see com.heavenize.hbase.QuoteDAO#get(java.lang.String)
   */
  @Override
  public Quote findBySymbol(String symbol) throws Exception
  {
    System.out.println("Querying table...");
    PreparedStatement statement = conn.prepareStatement("SELECT * FROM " + QuoteTable.TABLE_NAME + " WHERE symbol=?");    
    statement.setString(1, symbol);
    ResultSet rs = statement.executeQuery();
    Quote quote = null;
    if (rs.next())
    {
      String name = rs.getString("name");
      Double price = rs.getDouble("price");
      quote = new Quote(name, symbol, price);
    }
    return quote;
  }

  /* (non-Javadoc)
   * @see com.heavenize.hbase.QuoteDAO#put(com.heavenize.hbase.Quote)
   */
  @Override
  public void put(Quote q) throws Exception
  {
    throw new IllegalAccessError("Not yet implemented");
  }

  /* (non-Javadoc)
   * @see com.heavenize.hbase.QuoteDAO#get()
   */
  @Override
  public Iterable<Quote> get() throws Exception
  {
    throw new IllegalAccessError("Not yet implemented");
  }

  /* (non-Javadoc)
   * @see com.heavenize.hbase.QuoteDAO#createTable()
   */
  @Override
  public void createTable() throws Exception
  {
    System.out.println("Creating table for phoenix if it does not exist...");
    String tableCreationQuery = "CREATE TABLE IF NOT EXISTS "+QuoteTable.TABLE_NAME+" (symbol VARCHAR PRIMARY KEY, name VARCHAR, price DOUBLE)";
    Statement tableCreationStatement = conn.createStatement();
    tableCreationStatement.execute(tableCreationQuery);
  }

}
