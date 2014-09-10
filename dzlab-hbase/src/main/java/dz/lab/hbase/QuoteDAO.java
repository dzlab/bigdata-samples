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
public interface QuoteDAO
{
  void createTable() throws Exception;
  Quote findBySymbol(String key) throws Exception;
  void put(Quote q) throws Exception;
  Iterable<Quote> get() throws Exception;
  void close() throws Exception;
}
