package us.yuxin.hump;

public class StoreCounter {
  public long rows;
  public long inBytes;
  public long outBytes;
  public long nullCells;
  public long cells;
  public long during;

  public void plus(StoreCounter c) {
    rows += c.rows;
    inBytes += c.inBytes;
    outBytes += c.outBytes;
    nullCells += c.nullCells;
    cells += c.cells;
    during += c.during;
  }

  public void reset() {
    rows = 0;
    inBytes = 0;
    outBytes = 0;
    nullCells = 0;
    cells = 0;
    during = 0;
  }
}
