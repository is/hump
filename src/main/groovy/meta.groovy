@Grab(group ="org.apache.hadoop", module="hadoop-common", version="2.0.0-alpha")

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf)

// vim: ts=2 sts=2 ai