package us.yuxin.hump.cli;

import java.io.PrintStream;
import java.sql.SQLException;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import us.yuxin.hump.JdbcSource;
import us.yuxin.hump.JdbcSourceMetadata;

public class Gen {
  CommandLine cmdline;
  Options options;

  String errorMsg;

  PrintStream out;
  PrintStream err;

  String columnNames[];
  String columnTypes[];

  String tableName;

  // ----
  final static String O_COLUMNS = "columns";
  final static String O_COMMENT = "comment";
  final static String O_DRIVER = "driver";
  final static String O_EXTERNAL = "external";
  final static String O_FLAGS = "flags";
  final static String O_HIVE_TYPES = "hivetypes";
  final static String O_PASSWORD = "password";
  final static String O_PARTITION = "partition";
  final static String O_QUERY = "query";
  final static String O_SOURCE = "source";
  final static String O_SQL_TYPES = "sqltypes";
  final static String O_TABLE = "table";
  final static String O_TARGET = "target";
  final static String O_USERNAME = "username";


  private void prepareCmdlineOptions() {
    options = new Options();
    addOption("d", O_DRIVER, true, "JDBC Driver classname", "class");
    addOption("s", O_SOURCE, true, "Data source URL", "url");
    addOption("q", O_QUERY, true, "SQL query statement", "statement");
    addOption(null, O_TABLE, true, "Data source table name", "name");

    addOption("C", O_COLUMNS, true, "Column names", "names");
    addOption("H", O_HIVE_TYPES, true, "Column hive types", "types");
    addOption("S", O_SQL_TYPES, true, "Column SQL types", "types");

    addOption("t", O_TARGET, true, "Target table name", "name");
    addOption(null, O_COMMENT, true, "Table comments", "comment");

    addOption("u", O_USERNAME, true, "Database username", "user");
    addOption("p", O_PASSWORD, true, "Database password", "pass");

    addOption("f", O_FLAGS, true, "Flags for hive DML statement\ne:EXTERNAL, i:IF NOT EXIST, d:DROP TABLE", "flags");
    addOption("e", O_EXTERNAL, false, "External hive table");
    addOption("P", O_PARTITION, true, "Hive table partition, v:type,v:type ...");
  }


  public static void main(String argv[]) throws Exception {
    Gen app = new Gen();
    app.run(argv);
  }

  private void run(String args[]) throws Exception {

    prepareCmdlineOptions();

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("gen", options);
      System.exit(0);
    }

    parseCmdline(OptionsFileUtils.expandArguments(args));

    if (cmdline.hasOption(O_COLUMNS) && cmdline.hasOption(O_HIVE_TYPES)) {
      genByHiveColumnDescriptor();
    }

    if (cmdline.hasOption(O_SOURCE)) {
      genByDatasource();
    }
  }

  private void genByDatasource() throws ClassNotFoundException, SQLException {
    String url = cmdline.getOptionValue(O_SOURCE);
    String driver = cmdline.getOptionValue(O_DRIVER);
    String table = cmdline.getOptionValue(O_TABLE);
    String query = cmdline.getOptionValue(O_QUERY);

    if (driver.equals("mysql")) {
      driver = "com.mysql.jdbc.Driver";
    }

    if (query == null) {
      query = "SELECT * FROM " + table + " LIMIT 1;";
    }

    JdbcSource source = new JdbcSource();
    source.setDriver(driver);
    source.setUrl(url);
    source.setQuery(query);
    source.setUsername(cmdline.getOptionValue(O_USERNAME));
    source.setPassword(cmdline.getOptionValue(O_PASSWORD));
    source.open();

    JdbcSourceMetadata meta = new JdbcSourceMetadata();
    meta.setJdbcSource(source);

    columnNames = meta.names;
    columnTypes = meta.getHiveTypeNames();
    source.close();

    buildTable();
    // System.exit(0);
  }


  private void genByHiveColumnDescriptor() {
    String columns = cmdline.getOptionValue(O_COLUMNS);
    String hivetypes = cmdline.getOptionValue(O_HIVE_TYPES);

    columnNames = Iterables.toArray(Splitter.on(",").split(columns), String.class);
    columnTypes = Iterables.toArray(Splitter.on(":").split(hivetypes), String.class);

    if (columnNames.length != columnTypes.length) {
      errorMsg = "Column names/types is not match.\n" +
        "names: " + columns + " " + columnNames.length + "\n" +
        "types: " + hivetypes + " " + columnTypes.length + "\n";
      errorExit(1);
    }

    buildTable();
    // System.exit(0);
  }


  private void errorExit(int retcode) {
    prepareStream();

    if (errorMsg != null) {
      err.print(errorMsg);
      System.exit(retcode);
    }
  }


  private void prepareStream() {
    if (out == null)
      out = System.out;

    if (err == null)
      err = System.err;
  }


  private void prepareTablename() {
    if (tableName != null)
      return;

    tableName = cmdline.getOptionValue(O_TARGET);

    if (tableName == null)
      tableName = cmdline.getOptionValue(O_TABLE);

    if (tableName == null)
      tableName = "t0";
  }


  private void parseCmdline(String args[]) throws ParseException {
    GnuParser parser = new GnuParser();
    cmdline = parser.parse(options, args, null, false);
  }


  private void addOption(String shortOpt, String longOpt, boolean hasArg, String description) {
    Option o = new Option(shortOpt,longOpt, hasArg, description);
    options.addOption(o);
  }


  private void addOption(String shortOpt, String longOpt, boolean hasArg, String description, String argName) {
    Option o = new Option(shortOpt,longOpt, hasArg, description);
    o.setArgName(argName);
    options.addOption(o);
  }


  private void buildTable() {
    prepareStream();
    prepareTablename();

    String flags = cmdline.getOptionValue(O_FLAGS, "");


    if (flags.indexOf('d') != -1) {
      out.println("DROP TABLE IF EXISTS " + tableName + ";");
    }

    String external;
    String ifNotExists;
    if (flags.indexOf('e') != -1 || cmdline.hasOption(O_EXTERNAL))
      external = "EXTERNAL ";
    else
      external = "";

    if (flags.indexOf('i') != -1)
      ifNotExists = "IF NOT EXISTS ";
    else
      ifNotExists = "";


    out.println("CREATE " + external + "TABLE " + ifNotExists + tableName + " (");
    for (int i = 0; i < columnNames.length; ++i) {
      out.print(columnNames[i] + " " + columnTypes[i]);
      if (i != (columnNames.length - 1))
        out.println(",");
      else
        out.println(")");
    }

    if (cmdline.hasOption(O_COMMENT))
      out.println("COMMENT '" + cmdline.getOptionValue(O_COMMENT) + "'");

    if (cmdline.hasOption(O_PARTITION)) {
      String pstr = cmdline.getOptionValue(O_PARTITION);
      String speces[] = Iterables.toArray(Splitter.on(",").split(pstr), String.class);

      StringBuilder sb = new StringBuilder();
      sb.append("PARTITIONED BY (");
      for (int i = 0; i < speces.length; ++i) {
        String spec = speces[i];
        int p = spec.indexOf(":");
        if (p == -1) {
          sb.append(spec).append(" string");
        } else {
          sb.append(spec.replace(':', ' '));
        }

        if (i != speces.length - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      out.println(sb.toString());
    }

    out.println("STORED AS RCFile;");
  }

  void setOutStream(PrintStream out) {
    this.out = out;
  }

  void call(String argv[]) throws Exception {
    run(argv);
  }
}
