package dz.lab.mapred;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class StartsWithCountJobTest extends TestCase
{
  private File inputFile = new File("./target/test/input.txt");
  private File output    = new File("./target/test-result/");

  @Before
  public void setUpTest() throws IOException
  {
    FileUtils.deleteQuietly(inputFile);
    FileUtils.deleteQuietly(output);
    // write input text to the job under test
    FileUtils.write(inputFile, "this is just a test, yes it is");
  }

  @Test
  public void testRun() throws Exception
  {
    Configuration conf = new Configuration();
    // configure the job to run locally
    conf.set("mapreduce.framework.name", "local");
    // use local file system
    conf.set("fs.default.name", "file:///");

    StartsWithCountJob underTest = new StartsWithCountJob();
    underTest.setConf(conf);

    // execute the job
    int exitCode = underTest.run(new String[] { inputFile.getAbsolutePath(), output.getAbsolutePath() });
    assertEquals("Returned error code.", 0, exitCode);
    assertTrue(new File(output, "_SUCCESS").exists());

    // convert the reduce output into a map for verification
    Map<String, Integer> resAsMap = getResultAsMap(new File(output, "part-r-00000"));
    assertEquals(5, resAsMap.size());
    assertEquals(2, resAsMap.get("t").intValue());
    assertEquals(3, resAsMap.get("i").intValue());
    assertEquals(1, resAsMap.get("j").intValue());
    assertEquals(1, resAsMap.get("a").intValue());
    assertEquals(1, resAsMap.get("y").intValue());
  }

  private Map<String, Integer> getResultAsMap(File file) throws IOException
  {
    Map<String, Integer> result = new HashMap<String, Integer>();
    String contentOfFile = FileUtils.readFileToString(file);
    for (String line : contentOfFile.split("\n"))
    {
      String[] tokens = line.split("\t");
      result.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    return result;
  }
}
