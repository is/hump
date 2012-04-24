package us.yuxin.hump.testutils;

import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import org.hsqldb.cmdline.SqlFile;

public class DBTools {
  public static void initDatabase(Connection conn, String url) throws Exception {
    InputStreamReader isr = new InputStreamReader(DBTools.class.getResourceAsStream(url));
    SqlFile sqlFile = new SqlFile(isr, "stdin", System.out, null, false, new File("."));
    sqlFile.setConnection(conn);
    sqlFile.setAutoClose(false);
    sqlFile.execute();
  }
}
