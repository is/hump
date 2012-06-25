package us.yuxin.hump;

public class VirtualColumn {
  public String columnName;
  public String columnType;
  public Object defaultValue;

  public VirtualColumn(String columnName, String columnType, Object defaultValue) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.defaultValue = defaultValue;
  }
}
