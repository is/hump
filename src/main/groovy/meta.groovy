@Grab(group = "org.apache.hadoop", module = "hadoop-common", version= "2.0.0-alpha")
@Grab(group = "org.apache.avro", module = "avro", version = "1.5.4")
@Grab(group = "org.apache.avro", module = "avro-tools", version = "1.5.4")

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader


Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);
Path path = new Path(args[0]);

InputStream nis = fs.open(path);
DataFileStream dis = new DataFileStream<>(nis, new GenericDatumReader<Void>());
println dis.getSchema().toString(true);
dis.close();
