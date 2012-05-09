package us.yuxin.hump.cli;

import java.io.PrintStream;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
  final static String O_HIVE_TYPES = "hivetypes";
  final static String O_SOURCE = "source";
  final static String O_SQL_TYPES = "sqltypes";
  final static String O_TABLE = "table";
  final static String O_TARGET = "target";

  public static void main(String argv[]) throws Exception {
    Gen app = new Gen();
    app.run(argv);
  }

  private void run(String args[]) throws Exception {

    prepareCmdlineOptions();

    if (args.length == 0) {
      HelpFormatter formmatter = new HelpFormatter();
      formmatter.printHelp("gen", options);
      System.exit(0);
    }

    parseCmdline(OptionsFileUtil.expandArguments(args));

    if (cmdline.hasOption(O_COLUMNS) && cmdline.hasOption(O_HIVE_TYPES)) {
      genByHiveColumnDescripter();
    }
  }

  private void genByHiveColumnDescripter() {
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
    System.exit(0);
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


  private void prepareCmdlineOptions() {
    options = new Options();
    Option o;

    o = new Option("d", O_DRIVER, true, "JDBC Driver classname");
    o.setArgName("class");
    options.addOption(o);

    o = new Option("s", O_SOURCE, true, "Data source URL");
    o.setArgName("url");
    options.addOption(o);

    o = new Option(null, O_TABLE, true, "Data source table name");
    o.setArgName("name");
    options.addOption(o);

    o = new Option("C", O_COLUMNS, true, "Column names");
    o.setArgName("names");
    options.addOption(o);

    o = new Option("H", O_HIVE_TYPES, true, "Column hive types");
    o.setArgName("types");
    options.addOption(o);

    o = new Option("S", O_SQL_TYPES, true, "Column SQL types");
    o.setArgName("types");
    options.addOption(o);

    o = new Option("t", O_TARGET, true, "Target table name");
    o.setArgName("name");
    options.addOption(o);

    o = new Option(null, O_COMMENT, true, "Table comments");
    o.setArgName("comment");
    options.addOption(o);
  }


  private void parseCmdline(String args[]) throws ParseException {
    GnuParser parser = new GnuParser();
    cmdline = parser.parse(options, args, null, false);
  }

  private void buildTable() {
    prepareStream();
    prepareTablename();

    out.println("CREATE TABLE " + tableName + " (");
    for (int i = 0; i < columnNames.length; ++i) {
      out.print(columnNames[i] + " " + columnTypes[i]);
      if (i != (columnNames.length - 1))
        out.println(",");
      else
        out.println(")");
    }

    if (cmdline.hasOption(O_COMMENT))
      out.println("COMMENT '" + cmdline.getOptionValue(O_COMMENT) + "'");

    out.println("STORED AS RCFile;");
  }
}
