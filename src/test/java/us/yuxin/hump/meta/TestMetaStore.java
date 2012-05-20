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


  @Test
  public void testMetaStore() throws ClassNotFoundException, SQLException, IOException {
    String jdbcURL = "jdbc:hsqldb:file:test-tmp/test;shutdown=true";
    try {
      FileUtils.cleanDirectory(new File("test-tmp"));
    } catch(IllegalArgumentException ex) {}

    MetaStore store = new MetaStore();
    store.open(jdbcURL);
    store.createSchema();

    PieceDao piece = new PieceDao();

    piece.id = "test0";
    piece.name = "arena";
    piece.category = "rrlstx";
    piece.label1 = "x1";
    piece.label2 = "20110305";

    Assert.assertTrue(store.save(piece));
    piece.label2 = "20110306";
    Assert.assertFalse(store.save(piece));

    piece = new PieceDao();
    Assert.assertFalse(store.load(piece, "test1"));
    Assert.assertTrue(store.load(piece, "test0"));
    Assert.assertEquals("20110306", piece.label2);

    store.close();
  }
}
