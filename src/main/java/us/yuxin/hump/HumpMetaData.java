package us.yuxin.hump;

import java.sql.Types;
import java.util.HashMap;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

public class HumpMetaData {
  public int columnCount;
  public String names[];
  public String typeNames[];
  public int types[];


  static ImmutableMap<Integer, String> hiveTypeMap;

  static {
    hiveTypeMap = ImmutableMap.<Integer, String>builder()
      .put(Types.BIT, "boolean")
      .put(Types.BOOLEAN, "boolean")

      .put(Types.TINYINT, "tinyint")
      .put(Types.SMALLINT, "smallint")
      .put(Types.INTEGER, "int")
      .put(Types.BIGINT, "bigint")

      .put(Types.FLOAT, "float")

      .put(Types.NUMERIC, "double")
      .put(Types.DECIMAL, "double")
      .put(Types.REAL, "double")
      .put(Types.DOUBLE, "double")

      .put(Types.DATE, "timestamp")
      .put(Types.TIMESTAMP, "timestamp")

      .put(Types.TIME, "string")
      .put(Types.VARCHAR, "string")
      .put(Types.LONGVARCHAR, "string")
      .put(Types.NVARCHAR, "string")
      .put(Types.NCHAR, "string")
      .put(Types.LONGNVARCHAR, "string")
      .put(Types.CLOB, "string")

      .build();
  }

  /**
   * Initialize Hump metadata by column count.
   *
   * @param count Column count
   */
  public void init(int count) {
    this.columnCount = count;

    this.names = new String[count];
    this.typeNames = new String[count];
    this.types = new int[count];
  }


  public String[] getHiveTypeNames() {
    String hiveTypeNames[] = new String[columnCount];
    for (int c = 0; c < columnCount; ++c) {
      hiveTypeNames[c] = hiveTypeMap.get(types[c]);
    }
    return hiveTypeNames;
  }


  public void fillRCFileColumns(Properties tbl) {
    tbl.setProperty("columns", Joiner.on(',').join(names));
    tbl.setProperty("columns.types", Joiner.on(':').join(getHiveTypeNames()));
  }
}
