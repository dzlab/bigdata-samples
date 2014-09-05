/**
 * (c) Copyright 2005-2014 Heavenize SAS
 * 34, rue serpente, 75006 Paris, FRANCE
 * HEAVENIZE project
 *
 * This code is the property of Heavenize SAS
 * Registration : RCS PARIS B 508 496 528
 * For any question or license, please contact Heavenize at info@heavenize.com
 */
package com.heavenize.samples.curator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.rest.webmvc.config.RepositoryRestMvcConfiguration;

/**
 * @author dzlab (dzlabs@outlook.com) , 14 août 2014
 */
@ComponentScan
@EnableAutoConfiguration
@Import(RepositoryRestMvcConfiguration.class)
@ImportResource("file:src/main/webapp/WEB-INF/mvc-dispatcher-servlet.xml")
public class Application
{
  /**
   * Logger for class {@link Application}.
   */
  private final static Logger log = LoggerFactory.getLogger(Application.class);

  public static void main(String[] args)
  {
    SpringApplication.run(Application.class, args);

  }
}
