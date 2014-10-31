package dz.lab.curator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.rest.webmvc.config.RepositoryRestMvcConfiguration;

/**
 * @author dzlab (dzlabs@outlook.com) , 14 ao�t 2014
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
