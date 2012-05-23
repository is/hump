package us.yuxin.hump.cli;

import org.junit.Assert;
import org.junit.Test;

import static us.yuxin.hump.cli.ConditionUtils.dateCondition;
import static us.yuxin.hump.cli.ConditionUtils.rangeCondition;

public class TestConditionUtils {
  @Test
  public void testRange() {
    Assert.assertArrayEquals(new String[] {"x1"}, ConditionUtils.range("x1"));
    Assert.assertArrayEquals(new String[] {"x1","x2","x3"},
      ConditionUtils.range("x1, x3,x2"));
    Assert.assertArrayEquals(new String[] {"x2","x4","x5", "x6"},
      ConditionUtils.range("x2, x4-6"));

    Assert.assertEquals("label1 = 'x1'", rangeCondition("x1"));
    Assert.assertEquals("", rangeCondition(""));
    Assert.assertEquals("label1 in ('x1', 'x4', 'x5', 'x6')", rangeCondition("x1,x4-6"));
  }


  @Test
  public void testDate() {
    dateEquals("", "");
    dateEquals("(label2 = '20120513')", "20120513");
    dateEquals("((label2 = '20120513') OR (label2 = '20120515'))", "20120513,20120515");
    dateEquals("(label2 >= '20120400' AND label2 <= '20120440')", "201204");
    dateEquals("(label2 >= '20120400' AND label2 <= '20120440')", "04");
    dateEquals(
      "((label2 >= '20120400' AND label2 <= '20120440') OR (label2 >= '20120500' AND label2 <= '20120540'))",
      "04,05");

    dateEquals("(label2 >= '20120304' AND label2 <= '20120510')", "0304-0510");
    dateEquals("((label2 >= '20120304' AND label2 <= '20120510') OR (label2 >= '20110000' AND label2 <= '20151240'))",
      "0304-0510,2011-2015");

  }

  private static void dateEquals(String s1, String s2) {
    Assert.assertEquals(s1, dateCondition(s2));
  }
}
