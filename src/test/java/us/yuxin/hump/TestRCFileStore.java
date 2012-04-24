package us.yuxin.hump;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import us.yuxin.hump.testutils.DBTools;

public class TestRCFileStore {
  Connection connection;
  static String driver = "org.hsqldb.jdbcDriver";
  static String url = "jdbc:hsqldb:mem:UnitTestJdbcSource";

  @Before
  public void setUp() {
    try {
      Class.forName(driver);
      connection = DriverManager.getConnection(url);

    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during HSQL database startup.");
    }
  }

  @After
  public void tearDown() {
    try {
      connection.createStatement().execute("SHUTDOWN");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Shutdown HSQL database");
    }
  }

  @Test
  public void testRCFileStore() throws Exception, ClassNotFoundException, SQLException {
    DBTools.initDatabase(connection, "/us/yuxin/hump/testdb/JdbcSource0.sql");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    RCFileStore store = new RCFileStore(fs, conf, null);

    JdbcSource source = new JdbcSource();
    source.setDriver(driver);
    source.setUrl(url);
    source.setUsername("sa");
    source.setQuery("SELECT * FROM t0");
    source.open();
    store.store(new Path("mapred/RCFileStore1"), source, null);
    source.close();
  }
}
