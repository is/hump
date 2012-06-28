@Grab(group = "org.apache.hadoop", module = "hadoop-common", version= "2.0.0-alpha")
@Grab(group = "org.apache.avro", module = "avro", version = "1.5.4")
@Grab(group = "org.apache.avro", module = "avro-tools", version = "1.5.4")

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader

String tablename = args[0]
String location = args[1]
Path path = new Path(args[1]);

Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);

InputStream nis = fs.open(path);
DataFileStream dis = new DataFileStream<>(nis, new GenericDatumReader<Void>());
String schema = dis.getSchema().toString(false);
dis.close();

println """CREATE EXTERNAL TABLE ${tablename}
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${location}'
TBLPROPERTIES ('avro.schema.literal'='${schema}');"""