package us.yuxin.hump.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Gen {
  CommandLine cmdline;
  Options options;

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

    if (cmdline.hasOption("columns")) {
      genByColumnDescripter();
    }
  }

  private void genByColumnDescripter() {
  }

  private void prepareCmdlineOptions() {
    options = new Options();
    Option o;

    o = new Option("d", "driver", true, "JDBC Driver classname");
    o.setArgName("class");
    options.addOption(o);

    o = new Option("s", "source", true, "Data source URL");
    o.setArgName("url");
    options.addOption(o);

    o = new Option(null, "table", true, "Data source table name");
    o.setArgName("name");
    options.addOption(o);

    o = new Option("C", "columns", true, "Column names");
    o.setArgName("names");
    options.addOption(o);

    o = new Option("H", "hivetypes", true, "Column hive types");
    o.setArgName("types");
    options.addOption(o);

    o = new Option("S", "sqltypes", true, "Column SQL types");
    o.setArgName("types");
    options.addOption(o);

    o = new Option("t", "target", true, "Target table name");
    o.setArgName("name");
    options.addOption(o);
  }


  private void parseCmdline(String args[]) throws ParseException {
    GnuParser parser = new GnuParser();
    cmdline = parser.parse(options, args, null, false);
  }
}
