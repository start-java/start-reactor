package tech.simter.start.reactor;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author RJ
 */
class SampleTest {
  private Logger logger = LoggerFactory.getLogger(SampleTest.class);

  @Test
  void test() {
    logger.debug("test");
  }
}