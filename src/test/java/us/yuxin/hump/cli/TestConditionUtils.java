package us.yuxin.hump.cli;

import org.junit.Assert;
import org.junit.Test;

public class TestConditionUtils {
  @Test
  public void testRange() {
    Assert.assertArrayEquals(new String[] {"x1"}, ConditionUtils.ranges("x1"));
    Assert.assertArrayEquals(new String[] {"x1","x2","x3"},
      ConditionUtils.ranges("x1, x3,x2"));
    Assert.assertArrayEquals(new String[] {"x2","x4","x5", "x6"},
      ConditionUtils.ranges("x2, x4-6"));

    Assert.assertEquals("label1 = 'x1'", ConditionUtils.rangesCondition("x1"));
    Assert.assertEquals("", ConditionUtils.rangesCondition(""));
    Assert.assertEquals("label1 in ('x1', 'x4', 'x5', 'x6')", ConditionUtils.rangesCondition("x1,x4-6"));
  }
}
