package us.yuxin.hump;

public class HumpMetaData {
  public int columnCount;
  public String names[];
  public String typeNames[];
  public int types[];


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
}
