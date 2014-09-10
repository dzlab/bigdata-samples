/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package dz.lab.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 8 sept. 2014
 */
public class HiveJDBCClient
{
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  /**
   * @param args
   * @throws SQLException 
   */
  public static void main(String[] args) throws SQLException
  {
    try
    {
      Class.forName(driverName);
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    ResultSet res;
    Connection con = DriverManager.getConnection("jdbc:hive://192.168.2.72:10000/default", "", "");
    Statement stmt = con.createStatement();
    
    System.out.println("create table tablename");
    String tableName = "test1";
    res = stmt.executeQuery("create table if not exists " + tableName + " (key INT, value STRING) row format delimited fields terminated by '\t' stored as textfile");
    
    System.out.println("show tables");
    String sql = "show tables '" + tableName + "'";
    res = stmt.executeQuery(sql);
    print(res);
    
    System.out.println("describe table");
    sql = "describe " + tableName;
    res = stmt.executeQuery(sql);

    System.out.println("load data into table");
    // NOTE: filepath has to be local to the hive server
    String filepath = "/home/hadoop/sample.txt";
    sql = "load data local inpath '" + filepath + "' overwrite into table "+tableName;
    res = stmt.executeQuery(sql);
    
    System.out.println("select * query");
    sql = "select * from " + tableName;
    res = stmt.executeQuery(sql);
    print(res);
    
    System.out.println("truncate table");
    sql = "truncate table " + tableName;
    res = stmt.executeQuery(sql);
    
    System.out.println("drop table");
    sql = "drop table " + tableName;
    res = stmt.executeQuery(sql);
    
    System.out.println("Done.");
  }

  private static void print(ResultSet res) throws SQLException
  {
    ResultSetMetaData rsmd = res.getMetaData();
    while (res.next()) {
      System.out.print("{");
      for(int i=1; i<=rsmd.getColumnCount(); i++)
      {
        System.out.print("'"+rsmd.getColumnLabel(i)+"': " + res.getString(i)+", ");
      }
      System.out.println("}");
    }
  }
}
