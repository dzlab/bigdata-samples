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

/**
 * 
 * @author Antoine Larcher (antoine.larcher@heavenize.com) , 2 sept. 2014
 */
public class Quote
{
  String name;
  String symbol;
  Double price;
  /**
   * @param name
   * @param symbol
   * @param price
   */
  public Quote(String name, String symbol, Double price)
  {
    this.name = name;
    this.symbol = symbol;
    this.price = price;
  }
  
  @Override
  public String toString()
  {
    return "Quote [name=" + name + ", symbol=" + symbol + ", price=" + price + "]";
  }    
  
}
