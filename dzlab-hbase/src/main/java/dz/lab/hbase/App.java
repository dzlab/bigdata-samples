package dz.lab.hbase;


/**
 * Hello world!
 */
public class App
{
  public static void main(String[] args) throws Exception
  {        
    System.out.println("Manipulating Quotes table with the HBase API");
    QuoteDAO hbaseDao = new QuoteHbaseDAO();
    hbaseDao.createTable();
    hbaseDao.put(new Quote("General Electric", "GE", 28.09));
    hbaseDao.put(new Quote("Google Inc.", "GOOG", 604.83));
    hbaseDao.put(new Quote("Facebook, Inc.", "FB", 72.59));
    System.out.println("Quote with key 'GOOG': " + hbaseDao.findBySymbol("GOOG"));
    hbaseDao.close();
    
    System.out.println("Manipulating Quotes table with the Phoenix API");
    QuoteDAO phoenixDao = new QuotePhoenixDAO();
    phoenixDao.createTable();
    System.out.println("Quote with key 'FB': " + phoenixDao.findBySymbol("FB"));
    phoenixDao.close();
    
    System.out.println("Bye.");
  }
}
