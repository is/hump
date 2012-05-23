package us.yuxin.hump.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import us.yuxin.hump.meta.MetaStore;
import us.yuxin.hump.meta.dao.PieceDao;

public class Meta {
  CommandLine cmdline;
  Options options;

  final static String O_INIT = "init";
  final static String O_IMPORT = "import";
  final static String O_STORE = "store";
  final static String O_STATISTIC = "stat";

  final static String O_GENERATE = "generate";
  final static String O_CATEGORY = "category";
  final static String O_TARGET = "target";
  final static String O_NAME = "name";
  final static String O_RANGE = "range";
  final static String O_DATE = "date";
  final static String O_SCHEMA = "schema";
  final static String O_G1 = "g1";
  final static String O_CONF = "conf";

  final static String DEFAULT_STORE_PATH = "conf/.meta";
  final static String DEFAULT_DB_OPTIONS = ";LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0";


  // ---- for hive schema generator
  List<PieceDao> pieces;


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

    parseCmdline(OptionsFileUtils.expandArguments(args));

    if (cmdline.hasOption(O_INIT)) {
      initMateStore();
    } else if (cmdline.hasOption(O_IMPORT)) {
      importMateStore();
    } else if (cmdline.hasOption(O_STATISTIC)) {
      statisticPieces();
    } else if (cmdline.hasOption(O_GENERATE)) {
      generateHiveSchema();
    }
  }


  private void generateHiveSchema() throws ClassNotFoundException, SQLException {
    String schema = generateHiveSchemaString();
    System.out.println(schema);
  }


  private List<PieceDao> buildPieceList() throws ClassNotFoundException, SQLException {
    MetaStore store = getMetaStore();
    List<PieceDao> pieces;

    String rangeConditon = ConditionUtils.rangeCondition(cmdline.getOptionValue(O_RANGE, ""));
    String dateCondition = ConditionUtils.dateCondition(cmdline.getOptionValue(O_DATE, ""));
    String name = cmdline.getOptionValue(O_NAME);

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM piece WHERE name = '").append(name).append("'");

    if (rangeConditon.length() > 0)
      sb.append(" AND ").append(rangeConditon);

    if (dateCondition.length() > 0)
      sb.append(" AND ").append(dateCondition);

    sb.append(" ORDER BY name, label2, label1");
    String query = sb.toString();
    System.out.println("-- QUERY: " + query);

    pieces = store.getPieces(query);
    store.close();

    System.out.println("-- PIECES:" + pieces.size());
    return pieces;
  }


  private String generateHiveSchemaString() throws ClassNotFoundException, SQLException {
    // TODO
    pieces = buildPieceList();
    PieceDao piece = pieces.get(pieces.size() - 1);

    Gen gen = new Gen();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    String tablename = cmdline.getOptionValue(O_TARGET, cmdline.getOptionValue(O_NAME));
    gen.setOutStream(ps);
    String[] params = new String[] {
      "gen", "-f", "id", "-e", "-P", "s:string,d:string",
      "-C", piece.columns, "-H", piece.hivetypes,
      "-t", tablename,
    };

    try {
      gen.call(params);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }


//    ps.format("ALTER TABLE %s ADD \n", tablename);
//    for (PieceDao p: pieces) {
//      ps.format("PARTITION (s='%s', d='%s') location '%s' \n",
//        p.label1, p.label2, p.target.replaceFirst("/rcfile", ""));
//    }
//    ps.println(";");

    for (PieceDao p: pieces) {
      ps.format("ALTER TABLE %s ADD IF NOT EXISTS\n PARTITION (s='%s', d='%s') location '%s'; \n",
        tablename, p.label1, p.label2, p.target.replaceFirst("/rcfile", ""));
    }

    return baos.toString();
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


  private void statisticPieces() throws SQLException, ClassNotFoundException {
    MetaStore store = getMetaStore();
    pieceStatistic(store, "name", "schema");
    pieceStatistic(store, "label1", "range");
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
    store.open("jdbc:h2:" + dbPath + DEFAULT_DB_OPTIONS);
    return store;
  }


  private void prepareCmdlineOptions() {
    options = new Options();
    addOption("I", O_INIT, false, "JDBC Driver classname");
    addOption("i", O_IMPORT, false, "Import log file");
    addOption("S", O_STORE, true, "Metastore path", "meta");
    addOption("s", O_STATISTIC, "Pieces statistic");

    addOption("g", O_GENERATE, "Generate hive table schema");
    addOption(null, O_CATEGORY, true, "Specify category", "category");
    addOption("n", O_NAME, true, "Table name", "table");
    addOption("t", O_TARGET, true, "Target table name", "table");
    addOption("r", O_RANGE,  true, "Piece range", "range");
    addOption("d", O_DATE, true, "Date range", "date");
    addOption(null, O_SCHEMA, true, "Specify schema", "schema");

    addOption("c", O_CONF, true, "Config dir postfix", "config");
    addOption(null, O_G1, "Generate action one");
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
