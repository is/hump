package us.yuxin.hump;


import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;

public class LogFileUtils {
  public static FileWriter open(String fn0) {
    try {
      return new FileWriter(fn0);
    } catch (IOException ex) {
      return null;
    }
  }


  public static void close(FileWriter fw) {
    if (fw == null)
      return;

    try {
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeln(FileWriter fw, String str) {
    if (fw == null)
      return;

    try {
      fw.write(str);
      fw.write("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writelnWithTS(FileWriter fw, String str) {
    if (fw == null)
      return;

    String ts = String.format("%1$ty/%1$tm/%1$te %1$tH:%1$tM:%1$tS ", new GregorianCalendar());
    try {
      fw.write(ts);
      fw.write(str);
      fw.write("\n");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
