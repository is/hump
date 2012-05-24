package us.yuxin.hump.io;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;


public class SymlinkRCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable>
  extends RCFileInputFormat<K, V> {

  long minSplitSize = SequenceFile.SYNC_INTERVAL;
  long blockSize = 1024 * 1024 * 64;
  final static double SPLIT_SLOP = 1.1;


  public SymlinkRCFileInputFormat() {
    super();
  }



  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf, ArrayList<FileStatus> files) throws IOException {
    return true;
  }


  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Path[] symlinksDirs = getInputPaths(job);

    if (symlinksDirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    List<FileInfo> targetPaths = new ArrayList<FileInfo>();
    getTargetPathsFromSymlinksDirs(job, symlinksDirs, targetPaths);

    job.setLong("mapreduce.input.num.files", targetPaths.size());
    long totalSize = 0;

    for (FileInfo fi: targetPaths) {
      totalSize += fi.length;
    }

    long goalSize = totalSize / (numSplits == 0 ? 1: numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1), minSplitSize);

    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

    for (FileInfo fi: targetPaths) {
      Path path = fi.path;
      long length = fi.length;

      if (length != 0 ) /*isSplitable is  always true */ {
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);
        long bytesRemaining = length;

        while(((double)bytesRemaining) / splitSize > SPLIT_SLOP) {
          splits.add(new FileSplit(path, length - bytesRemaining, splitSize, new String[0]));
          bytesRemaining -= splitSize;
        }

        if (bytesRemaining != 0) {
          splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, new String[0]));
        }
      }
      LOG.debug("Total # of splits:" + splits.size());
      return splits.toArray(new FileSplit[splits.size()]);
    }

    return null;
  }


  private static void getTargetPathsFromSymlinksDirs(
    Configuration conf, Path[] symlinksDirs,
    List<FileInfo> targetPaths) throws IOException {

    for (Path symlinkDir: symlinksDirs) {
      FileSystem fileSystem = symlinkDir.getFileSystem(conf);
      FileStatus[] symlinks = fileSystem.listStatus(symlinkDir);

      for (FileStatus symlink: symlinks) {
        BufferedReader reader = new BufferedReader(
          new InputStreamReader(fileSystem.open(symlink.getPath())));

        String line;
        line = reader.readLine();
        if (line.equals("SYMLINK.RCFILE.V1")) {
          while((line = reader.readLine()) != null) {
            int o1 = line.indexOf(',');
            // TODO error handle.
            FileInfo fi = new FileInfo();
            fi.length = Long.parseLong(line.substring(0, o1));
            int o2 = line.indexOf(',', o1 + 1);
            fi.row = Long.parseLong(line.substring(o1, o2));
            fi.path = new Path(line.substring(o2 + 1));
            fi.symlink = symlink.getPath();

            // Skip zero row RCFile.
            if (fi.row == 0) {
              continue;
            }
            targetPaths.add(fi);
          }
        }
        reader.close();
      }
    }

  }

  public static class FileInfo {
    public Path path;
    public Path symlink;
    public long length;
    public long row;
  }
}
