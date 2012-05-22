package us.yuxin.hump.cli;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import us.yuxin.hump.meta.MetaStore;

public class Meta {
  CommandLine cmdline;
  Options options;

  final static String O_INIT = "init";
  final static String O_IMPORT = "import";
  final static String O_STORE = "store";
  final static String O_STATISTIC = "stat";

  final static String DEFAULT_STORE_PATH = "conf/meta/";


  public static void main(String argv[]) throws Exception {
    Meta app = new Meta();
    app.run(argv);
  }


  private void run(String args[]) throws Exception {
    prepareCmdlineOptions();

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("meta", options);
      System.exit(0);
    }

    parseCmdline(OptionsFileUtil.expandArguments(args));

    if (cmdline.hasOption(O_INIT)) {
      initMateStore();
    } else if (cmdline.hasOption(O_IMPORT)) {
      importMateStore();
    } else if (cmdline.hasOption(O_STATISTIC)) {
      pieceStatistic();
    }
  }



  private static void pieceStatistic(MetaStore store, String column, String title) throws SQLException {
    String values[] = store.getPieceStatisticByColumn(column);

    System.out.format("-- %s:%s -- \n", title.toUpperCase(), column);

    String oline = "   ";
    for (int i = 0; i < values.length; ++i) {
      oline += values[i];

      if (i != values.length - 1) {
        oline += " ";
      }
      if (oline.length() >= 70) {
        System.out.println(oline);
        oline = "   ";
      }
    }

    if (oline.length() > 3)
      System.out.println(oline);
    System.out.println();
  }


  private void pieceStatistic() throws SQLException, ClassNotFoundException {
    MetaStore store = getMetaStore();
    pieceStatistic(store, "name", "name");
    pieceStatistic(store, "label1", "origin");
    pieceStatistic(store, "label2", "date");
    store.close();
  }


  private void importMateStore() throws ClassNotFoundException, SQLException, IOException {
    MetaStore store = getMetaStore();
    store.importSummaryLog(cmdline.getArgs()[0]);
    store.updatePieceStatistic();
    store.close();
  }


  private void initMateStore() throws SQLException, ClassNotFoundException {
    MetaStore store = getMetaStore();
    store.createSchema();
    store.close();
  }

  private MetaStore getMetaStore() throws ClassNotFoundException, SQLException {
    MetaStore store = new MetaStore();

    String dbPath = cmdline.getOptionValue(O_STORE, DEFAULT_STORE_PATH);
    store.open("jdbc:hsqldb:file:" + dbPath + ";shutdown=true");
    return store;
  }


  private void prepareCmdlineOptions() {
    options = new Options();
    addOption("I", O_INIT, false, "JDBC Driver classname");
    addOption("i", O_IMPORT, false, "Import log file");
    addOption("S", O_STORE, true, "Metastore path", "meta");
    addOption("s", O_STATISTIC, "Pieces statistic");
  }


  private void addOption(String shortOpt, String longOpt, boolean hasArg, String description, String argName) {
    Option o = new Option(shortOpt, longOpt, hasArg, description);
    o.setArgName(argName);
    options.addOption(o);
  }


  private void addOption(String shortOpt, String longOpt, String description) {
    addOption(shortOpt, longOpt, false, description);
  }

  private void addOption(String shortOpt, String longOpt, boolean hasArg, String description) {
    Option o = new Option(shortOpt, longOpt, hasArg, description);
    options.addOption(o);
  }


  private void parseCmdline(String args[]) throws ParseException {
    GnuParser parser = new GnuParser();
    cmdline = parser.parse(options, args, null, false);
  }
}
