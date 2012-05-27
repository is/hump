package us.yuxin.hump.cli;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class CriteriaUtils {

  private static Pattern range0Pattern = Pattern.compile("(\\D)(\\d+?)-(\\d+?)"); // x1-23
  @SuppressWarnings("FieldCanBeLocal")
  private static String rangeColumnName = "p.label1";
  @SuppressWarnings("FieldCanBeLocal")
  private static String dateColumnName = "p.label2";

  /**
   * Handle range form like x3-12
   *
   * @param rs Range List
   * @param r Range Token
   */
  private static boolean range0(List<String> rs, String r) {
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


  public static String[] range(String in) {
    // basic form: x1, x2, x3, x4
    // range form: x1-5, n3-7
    List<String> rs = new ArrayList<String>();

    for (String s: Splitter.on(",").trimResults().omitEmptyStrings().split(in)) {
      s = s.trim();
      if (range0(rs, s))
        continue;

      rs.add(s);
    }

    Collections.sort(rs);
    return Iterables.toArray(rs, String.class);
  }

  public static String rangeCondition(String in) {
    String rs[] = range(in);
    if (rs.length == 0)
      return "";

    if (rs.length == 1)
      return String.format("%s = '%s'", rangeColumnName, rs[0]);

    StringBuilder sb = new StringBuilder();

    sb.append(rangeColumnName).append(" in (");
    for (int i = 0; i < rs.length; ++i) {
      sb.append("'").append(rs[i]).append("'");
      if (i != rs.length - 1)
        sb.append(", ");
    }
    sb.append(")");
    return sb.toString();
  }


  // form:
  //  2012 one year
  //  201203 one month
  //  04 one month
  //  0412 one day
  //
  //  20120301-20120321 full day range
  //  201203-201210 month range
  //  0304-0421 short day range
  //  2010-2012 year range
  //  03-11 short month range

  public static String dateCondition(String in) {
    List<String> conds = new LinkedList<String>();
    String thisYear = Integer.toString(Calendar.getInstance().get(Calendar.YEAR));

    for (String token: Splitter.on(",").trimResults().omitEmptyStrings().split(in)) {
      String subcond = null;

      if (token.indexOf('-') == -1) {
        if (token.length() == 8) {
          subcond = String.format("%s = '%s'", dateColumnName, token);
        } else if (token.length() == 6) {
          subcond = String.format(
            "%s >= '%s00' AND %s <= '%s40'", dateColumnName, token, dateColumnName, token);
        } else if (token.length() == 4) {
          if (token.compareTo("1800") > 0)
            subcond = String.format(
              "%s >= '%s0000' AND %s <= '%s1240'", dateColumnName, token, dateColumnName, token);
          else
            subcond = String.format(
              "%s = '%s%s'", dateColumnName, thisYear, token);
        } else if (token.length() == 2) {
           subcond = String.format(
             "%s >= '%s%s00' AND %s <= '%s%s40'",
             dateColumnName, thisYear, token, dateColumnName, thisYear, token);
        }
      } else {
        int o = token.indexOf('-');
        String tbegin = token.substring(0, o);
        String tend = token.substring(o + 1);

        if (tbegin.length() != tend.length())
          continue;


        if (tbegin.length() == 8) {
          subcond = String.format("%s >= '%s' AND %s <= '%s'",
            dateColumnName, tbegin, dateColumnName, tend);
        } else if (tbegin.length() == 6) {
          subcond = String.format(
            "%s >= '%s00' AND %s <= '%s40'", dateColumnName, tbegin, dateColumnName, tend);
        } else if (tbegin.length() == 4) {
          if (tbegin.compareTo("1800") > 0)
            subcond = String.format(
              "%s >= '%s0000' AND %s <= '%s1240'", dateColumnName, tbegin, dateColumnName, tend);
          else
            subcond = String.format(
              "%s >= '%s%s' AND %s <= '%s%s'",
              dateColumnName, thisYear, tbegin,
              dateColumnName, thisYear, tend);
        } else if (tbegin.length() == 2) {
          subcond = String.format(
            "%s >= '%s%s00' AND %s <= '%s%s40'",
            dateColumnName, thisYear, tbegin, dateColumnName, thisYear, tend);
        }
      }

      if (subcond != null)
        conds.add(subcond);
    }

    if (conds.size() == 0)
      return "";
    if (conds.size() == 1)
      return "(" + conds.get(0) + ")";

    StringBuilder sb = new StringBuilder();
    sb.append("((");
    Joiner.on(") OR (").appendTo(sb, conds);
    sb.append("))");
    return sb.toString();
  }
}
