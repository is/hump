package us.yuxin.hump.meta;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import us.yuxin.hump.meta.dao.PieceDao;

public class TestMetaStore {
  @Before
  public void setup() {

  }


  private void cleanDatabase(String path) throws IOException {
    try {
      FileUtils.cleanDirectory(new File(path));
    } catch(IllegalArgumentException ex) {}
  }


  private void cleanDatabase() throws IOException {
    try {
      FileUtils.cleanDirectory(new File("test-tmp"));
    } catch(IllegalArgumentException ex) {}
  }


  @Test
  public void testImportSummaryLogFull() throws ClassNotFoundException, SQLException, IOException {
    cleanDatabase("test-tmp/import/");

    String jdbcURL = "jdbc:hsqldb:file:test-tmp/import/;shutdown=true";
    MetaStore store;
    PieceDao piece = new PieceDao();

    store = new MetaStore();
    store.open(jdbcURL);
    store.createSchema();
    store.importSummaryLog("test-resources/hump-log-full");
    Assert.assertFalse(store.loadPiece(piece, "test3"));
    Assert.assertTrue(store.loadPiece(piece, "rrwar.x4.chat.20120318"));
    Assert.assertEquals(119, piece.rows);
    Assert.assertEquals(6232, piece.size);
    store.close();

    store.open(jdbcURL);
    Assert.assertFalse(store.loadPiece(piece, "test3"));
    Assert.assertTrue(store.loadPiece(piece, "rrwar.x4.chat.20120318"));
    Assert.assertEquals(119, piece.rows);
    Assert.assertEquals(6232, piece.size);
    store.close();
  }



  @Test
  public void testMetaStore() throws ClassNotFoundException, SQLException, IOException {
    cleanDatabase("test-tmp/test/");

    String jdbcURL = "jdbc:hsqldb:file:test-tmp/test/;shutdown=true";

    MetaStore store = new MetaStore();
    PieceDao piece = new PieceDao();
    store.open(jdbcURL);
    store.createSchema();
    piece.id = "test0";
    piece.name = "arena";
    piece.category = "rrlstx";
    piece.label1 = "x1";
    piece.label2 = "20110305";

    Assert.assertTrue(store.savePiece(piece));
    piece.label2 = "20110306";
    Assert.assertFalse(store.savePiece(piece));

    piece = new PieceDao();
    Assert.assertFalse(store.loadPiece(piece, "test1"));
    Assert.assertTrue(store.loadPiece(piece, "test0"));
    Assert.assertEquals("20110306", piece.label2);
    store.close();
  }
}
