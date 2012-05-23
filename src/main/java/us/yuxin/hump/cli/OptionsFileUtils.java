package us.yuxin.hump.cli;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OptionsFileUtils {
  private OptionsFileUtils() {
  }

  public static String[] expandArguments(String[] args) throws Exception {
    List<String> options = new ArrayList<String>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--options-file") || args[i].startsWith("@@")) {
        String fileName;

        if (args[i].startsWith("@@")) {
          fileName = args[i].substring(2);
        } else {

          if (i == args.length - 1) {
            throw new Exception("Missing options file");
          }
        }
        fileName = args[++i];


        File optionsFile = new File(fileName);
        BufferedReader reader = null;
        StringBuilder buffer = new StringBuilder();
        try {
          reader = new BufferedReader(new FileReader(optionsFile));
          String nextLine = null;
          while ((nextLine = reader.readLine()) != null) {
            nextLine = nextLine.trim();
            if (nextLine.length() == 0 || nextLine.startsWith("#")) {
              // empty line or comment
              continue;
            }

            buffer.append(nextLine);
            if (nextLine.endsWith("\\")) {
              if (buffer.charAt(0) == '\'' || buffer.charAt(0) == '"') {
                throw new Exception(
                  "Multiline quoted strings not supported in file("
                    + fileName + "): " + buffer.toString());
              }
              // Remove the trailing back-slash and continue
              buffer.deleteCharAt(buffer.length() - 1);
            } else {
              // The buffer contains a full option
              options.add(
                removeQuotesEncolosingOption(fileName, buffer.toString()));
              buffer.delete(0, buffer.length());
            }
          }

          // Assert that the buffer is empty
          if (buffer.length() != 0) {
            throw new Exception("Malformed option in options file("
              + fileName + "): " + buffer.toString());
          }
        } catch (IOException ex) {
          throw new Exception("Unable to read options file: " + fileName, ex);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
              ex.printStackTrace();
            }
          }
        }
      } else {
        // Regular option. Parse it and put it on the appropriate list
        options.add(args[i]);
      }
    }

    return options.toArray(new String[options.size()]);
  }

  /**
   * Removes the surrounding quote characters as needed. It first attempts to
   * remove surrounding double quotes. If successful, the resultant string is
   * returned. If no surrounding double quotes are found, it attempts to remove
   * surrounding single quote characters. If successful, the resultant string
   * is returned. If not the original string is returnred.
   *
   * @param fileName
   * @param option
   * @return
   * @throws Exception
   */
  private static String removeQuotesEncolosingOption(
    String fileName, String option) throws Exception {

    // Attempt to remove double quotes. If successful, return.
    String option1 = removeQuoteCharactersIfNecessary(fileName, option, '"');
    if (!option1.equals(option)) {
      // Quotes were successfully removed
      return option1;
    }

    // Attempt to remove single quotes.
    return removeQuoteCharactersIfNecessary(fileName, option, '\'');
  }

  /**
   * Removes the surrounding quote characters from the given string. The quotes
   * are identified by the quote parameter, the given string by option. The
   * fileName parameter is used for raising exceptions with relevant message.
   *
   * @param fileName
   * @param option
   * @param quote
   * @return
   * @throws Exception
   */
  private static String removeQuoteCharactersIfNecessary(String fileName,
                                                         String option, char quote) throws Exception {
    boolean startingQuote = (option.charAt(0) == quote);
    boolean endingQuote = (option.charAt(option.length() - 1) == quote);

    if (startingQuote && endingQuote) {
      if (option.length() == 1) {
        throw new Exception("Malformed option in options file("
          + fileName + "): " + option);
      }
      return option.substring(1, option.length() - 1);
    }

    if (startingQuote || endingQuote) {
      throw new Exception("Malformed option in options file("
        + fileName + "): " + option);
    }

    return option;
  }
}
