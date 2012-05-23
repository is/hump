package us.yuxin.hump.cli;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class ConditionUtils {

  private static Pattern range0Pattern = Pattern.compile("(\\D)(\\d+?)-(\\d+?)"); // x1-23


  private static String rangeColumnName = "label1";

  /**
   * Handle range form like x3-12
   *
   * @param rs
   * @param r
   */
  private static boolean ranges0(List<String> rs, String r) {
    Matcher matcher = range0Pattern.matcher(r);
    if (!matcher.matches())
      return false;

    String prefix = matcher.group(1);
    int begin = Integer.parseInt(matcher.group(2));
    int end = Integer.parseInt(matcher.group(3));
    for (int i = begin; i <= end; ++i) {
      rs.add(prefix + i);
    }
    return true;
  }


  public static String[] ranges(String in) {
    // basic form: x1, x2, x3, x4
    // range form: x1-5, n3-7
    List<String> rs = new ArrayList<String>();

    for (String s: Splitter.on(",").trimResults().omitEmptyStrings().split(in)) {
      s = s.trim();
      if (ranges0(rs, s))
        continue;

      rs.add(s);
    }

    Collections.sort(rs);
    return Iterables.toArray(rs, String.class);
  }

  public static String rangesCondition(String in) {
    String rs[] = ranges(in);
    if (rs.length == 0)
      return "";

    if (rs.length == 1)
      return String.format("%s = '%s'", rangeColumnName, rs[0]);

    StringBuilder sb = new StringBuilder();

    sb.append(rangeColumnName + " in (");
    for (int i = 0; i < rs.length; ++i) {
      sb.append("'" + rs[i] + "'");
      if (i != rs.length - 1)
        sb.append(", ");
    }
    sb.append(")");
    return sb.toString();
  }
}
