package us.yuxin.hump;

public class StoreCounter {
  public long rows;
  public long bytes;
  public long nullCells;
  public long cells;
  public long during;

  public void plus(StoreCounter c) {
    rows += c.rows;
    bytes += c.bytes;
    nullCells += c.nullCells;
    cells += c.cells;
    during += c.during;
  }

  public void reset() {
    rows = 0;
    bytes = 0;
    nullCells = 0;
    cells = 0;
    during = 0;
  }
}
