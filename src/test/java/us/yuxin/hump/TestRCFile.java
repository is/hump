/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package us.yuxin.hump;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * TestRCFile.
 *
 */
public class TestRCFile extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestRCFile.class);

  private static Configuration conf = new Configuration();

  private static ColumnarSerDe serDe;

  private static Path file;

  private static FileSystem fs;

  private static Properties tbl;

  static {
    try {
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test_rcfile");
      fs.delete(dir, true);
      // the SerDe part is from TestLazySimpleSerDe
      serDe = new ColumnarSerDe();
      // Create the SerDe
      tbl = createProperties();
      serDe.initialize(conf, tbl);
    } catch (Exception e) {
    }
  }

  // Data

  private static Writable[] expectedFieldsData = {
    new ByteWritable((byte) 123), new ShortWritable((short) 456),
    new IntWritable(789), new LongWritable(1000), new DoubleWritable(5.3),
    new Text("hive and hadoop"), null, null};

  private static Object[] expectedPartitalFieldsData = {null, null,
    new IntWritable(789), new LongWritable(1000), null, null, null, null};
  private static BytesRefArrayWritable patialS = new BytesRefArrayWritable();

  private static byte[][] bytesArray = null;

  private static BytesRefArrayWritable s = null;
  static {
    try {
      bytesArray = new byte[][] {"123".getBytes("UTF-8"),
        "456".getBytes("UTF-8"), "789".getBytes("UTF-8"),
        "1000".getBytes("UTF-8"), "5.3".getBytes("UTF-8"),
        "hive and hadoop".getBytes("UTF-8"), new byte[0],
        "NULL".getBytes("UTF-8")};
      s = new BytesRefArrayWritable(bytesArray.length);
      s.set(0, new BytesRefWritable("123".getBytes("UTF-8")));
      s.set(1, new BytesRefWritable("456".getBytes("UTF-8")));
      s.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      s.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      s.set(4, new BytesRefWritable("5.3".getBytes("UTF-8")));
      s.set(5, new BytesRefWritable("hive and hadoop".getBytes("UTF-8")));
      s.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      s.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

      // partial test init
      patialS.set(0, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(1, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(2, new BytesRefWritable("789".getBytes("UTF-8")));
      patialS.set(3, new BytesRefWritable("1000".getBytes("UTF-8")));
      patialS.set(4, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(5, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(6, new BytesRefWritable("NULL".getBytes("UTF-8")));
      patialS.set(7, new BytesRefWritable("NULL".getBytes("UTF-8")));

    } catch (UnsupportedEncodingException e) {
    }
  }

  public void testSimpleReadAndWrite() throws IOException, SerDeException {
    fs.delete(file, true);

    byte[][] record_1 = {"123".getBytes("UTF-8"), "456".getBytes("UTF-8"),
      "789".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
      "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
      new byte[0], "NULL".getBytes("UTF-8")};
    byte[][] record_2 = {"100".getBytes("UTF-8"), "200".getBytes("UTF-8"),
      "123".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
      "5.3".getBytes("UTF-8"), "hive and hadoop".getBytes("UTF-8"),
      new byte[0], "NULL".getBytes("UTF-8")};

    RCFileOutputFormat.setColumnNumber(conf, expectedFieldsData.length);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
      new DefaultCodec());
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record_1.length);
    for (int i = 0; i < record_1.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_1[i], 0,
        record_1[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    bytes.clear();
    for (int i = 0; i < record_2.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_2[i], 0,
        record_2[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    writer.close();

    Object[] expectedRecord_1 = {new ByteWritable((byte) 123),
      new ShortWritable((short) 456), new IntWritable(789),
      new LongWritable(1000), new DoubleWritable(5.3),
      new Text("hive and hadoop"), null, null};

    Object[] expectedRecord_2 = {new ByteWritable((byte) 100),
      new ShortWritable((short) 200), new IntWritable(123),
      new LongWritable(1000), new DoubleWritable(5.3),
      new Text("hive and hadoop"), null, null};

    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();

    for (int i = 0; i < 2; i++) {
      reader.next(rowID);
      BytesRefArrayWritable cols = new BytesRefArrayWritable();
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int j = 0; j < fieldRefs.size(); j++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(j));
        Object standardWritableData = ObjectInspectorUtils
          .copyToStandardObject(fieldData, fieldRefs.get(j)
            .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        if (i == 0) {
          assertEquals("Field " + i, standardWritableData, expectedRecord_1[j]);
        } else {
          assertEquals("Field " + i, standardWritableData, expectedRecord_2[j]);
        }
      }
    }

    reader.close();
  }

  public void testWriteAndFullyRead() throws IOException, SerDeException {
    writeTest(fs, 10000, file, bytesArray);
    fullyReadTest(fs, 10000, file);
  }

  public void testWriteAndPartialRead() throws IOException, SerDeException {
    writeTest(fs, 10000, file, bytesArray);
    partialReadTest(fs, 10000, file);
  }

  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 10000;
    boolean create = true;

    String usage = "Usage: RCFile " + "[-count N]" + " file";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    try {
      for (int i = 0; i < args.length; ++i) { // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else {
          // file is required parameter
          file = new Path(args[i]);
        }
      }

      if (file == null) {
        System.err.println(usage);
        System.exit(-1);
      }

      LOG.info("count = " + count);
      LOG.info("create = " + create);
      LOG.info("file = " + file);

      TestRCFile test = new TestRCFile();

      // test.performanceTest();

      test.testSimpleReadAndWrite();

      test.writeTest(fs, count, file, bytesArray);
      test.fullyReadTest(fs, count, file);
      test.partialReadTest(fs, count, file);
      System.out.println("Finished.");
    } finally {
      fs.close();
    }
  }

  private void writeTest(FileSystem fs, int count, Path file,
    byte[][] fieldsData) throws IOException, SerDeException {
    fs.delete(file, true);

    RCFileOutputFormat.setColumnNumber(conf, fieldsData.length);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
      new DefaultCodec());

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(fieldsData.length);
    for (int i = 0; i < fieldsData.length; i++) {
      BytesRefWritable cu = null;
      cu = new BytesRefWritable(fieldsData[i], 0, fieldsData[i].length);
      bytes.set(i, cu);
    }

    for (int i = 0; i < count; i++) {
      writer.append(bytes);
    }
    writer.close();
    long fileLen = fs.getFileStatus(file).getLen();
    System.out.println("The file size of RCFile with " + bytes.size()
      + " number columns and " + count + " number rows is " + fileLen);
  }

  private static Properties createProperties() {
    Properties tbl = new Properties();

    // Set the configuration parameters
    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    tbl.setProperty("columns",
      "abyte,ashort,aint,along,adouble,astring,anullint,anullstring");
    tbl.setProperty("columns.types",
      "tinyint:smallint:int:bigint:double:string:int:string");
    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
    return tbl;
  }

  public void fullyReadTest(FileSystem fs, int count, Path file)
    throws IOException, SerDeException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    ColumnProjectionUtils.setFullyReadColumns(conf);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    int actualRead = 0;
    BytesRefArrayWritable cols = new BytesRefArrayWritable();
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());
      for (int i = 0; i < fieldRefs.size(); i++) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils
          .copyToStandardObject(fieldData, fieldRefs.get(i)
            .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData, expectedFieldsData[i]);
      }
      // Serialize
      assertEquals(
        "Class of the serialized object should be BytesRefArrayWritable",
        BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedText = (BytesRefArrayWritable) serDe
        .serialize(row, oi);
      assertEquals("Serialized data", s, serializedText);
      actualRead++;
    }
    reader.close();
    assertEquals("Expect " + count + " rows, actual read " + actualRead,
      actualRead, count);
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }

  private void partialReadTest(FileSystem fs, int count, Path file)
    throws IOException, SerDeException {
    LOG.debug("reading " + count + " records");
    long start = System.currentTimeMillis();
    java.util.ArrayList<Integer> readCols = new java.util.ArrayList<Integer>();
    readCols.add(Integer.valueOf(2));
    readCols.add(Integer.valueOf(3));
    ColumnProjectionUtils.setReadColumnIDs(conf, readCols);
    RCFile.Reader reader = new RCFile.Reader(fs, file, conf);

    LongWritable rowID = new LongWritable();
    BytesRefArrayWritable cols = new BytesRefArrayWritable();

    while (reader.next(rowID)) {
      reader.getCurrentRow(cols);
      cols.resetValid(8);
      Object row = serDe.deserialize(cols);

      StructObjectInspector oi = (StructObjectInspector) serDe
        .getObjectInspector();
      List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
      assertEquals("Field size should be 8", 8, fieldRefs.size());

      for (int i : readCols) {
        Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
        Object standardWritableData = ObjectInspectorUtils
          .copyToStandardObject(fieldData, fieldRefs.get(i)
            .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
        assertEquals("Field " + i, standardWritableData,
          expectedPartitalFieldsData[i]);
      }

      assertEquals(
        "Class of the serialized object should be BytesRefArrayWritable",
        BytesRefArrayWritable.class, serDe.getSerializedClass());
      BytesRefArrayWritable serializedBytes = (BytesRefArrayWritable) serDe
        .serialize(row, oi);
      assertEquals("Serialized data", patialS, serializedBytes);
    }
    reader.close();
    long cost = System.currentTimeMillis() - start;
    LOG.debug("reading fully costs:" + cost + " milliseconds");
  }

  public void testSynAndSplit() throws IOException {
    splitBeforeSync();
    splitRightBeforeSync();
    splitInMiddleOfSync();
    splitRightAfterSync();
    splitAfterSync();
  }

  private void splitBeforeSync() throws IOException {
    writeThenReadByRecordReader(600, 1000, 2, 1, null);
  }

  private void splitRightBeforeSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17750, null);
  }

  private void splitInMiddleOfSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17760, null);

  }

  private void splitRightAfterSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 17770, null);
  }

  private void splitAfterSync() throws IOException {
    writeThenReadByRecordReader(500, 1000, 2, 19950, null);
  }

  private void writeThenReadByRecordReader(int intervalRecordCount,
    int writeCount, int splitNumber, long minSplitSize, CompressionCodec codec)
    throws IOException {
    Path testDir = new Path(System.getProperty("test.data.dir", ".")
      + "/mapred/testsmallfirstsplit");
    Path testFile = new Path(testDir, "test_rcfile");
    fs.delete(testFile, true);
    Configuration cloneConf = new Configuration(conf);
    RCFileOutputFormat.setColumnNumber(cloneConf, bytesArray.length);
    cloneConf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, intervalRecordCount);

    RCFile.Writer writer = new RCFile.Writer(fs, cloneConf, testFile, null, codec);

    BytesRefArrayWritable bytes = new BytesRefArrayWritable(bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      BytesRefWritable cu = null;
      cu = new BytesRefWritable(bytesArray[i], 0, bytesArray[i].length);
      bytes.set(i, cu);
    }
    for (int i = 0; i < writeCount; i++) {
      if (i == intervalRecordCount) {
        System.out.println("write position:" + writer.getLength());
      }
      writer.append(bytes);
    }
    writer.close();

    RCFileInputFormat inputFormat = new RCFileInputFormat();
    JobConf jonconf = new JobConf(cloneConf);
    jonconf.set("mapred.input.dir", testDir.toString());
    jonconf.setLong("mapred.min.split.size", minSplitSize);
    InputSplit[] splits = inputFormat.getSplits(jonconf, splitNumber);
    assertEquals("splits length should be " + splitNumber, splits.length, splitNumber);
    int readCount = 0;
    for (int i = 0; i < splits.length; i++) {
      int previousReadCount = readCount;
      RecordReader rr = inputFormat.getRecordReader(splits[i], jonconf, Reporter.NULL);
      Object key = rr.createKey();
      Object value = rr.createValue();
      while (rr.next(key, value)) {
        readCount++;
      }
      System.out.println("The " + i + "th split read "
        + (readCount - previousReadCount));
    }
    assertEquals("readCount should be equal to writeCount", readCount, writeCount);
  }


  // adopted Hadoop-5476 (calling new SequenceFile.Reader(...) leaves an
  // InputStream open, if the given sequence file is broken) to RCFile
  private static class TestFSDataInputStream extends FSDataInputStream {
    private boolean closed = false;

    private TestFSDataInputStream(InputStream in) throws IOException {
      super(in);
    }

    public void close() throws IOException {
      closed = true;
      super.close();
    }

    public boolean isClosed() {
      return closed;
    }
  }

  public void testCloseForErroneousRCFile() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    // create an empty file (which is not a valid rcfile)
    Path path = new Path(System.getProperty("test.build.data", ".")
      + "/broken.rcfile");
    fs.create(path).close();
    // try to create RCFile.Reader
    final TestFSDataInputStream[] openedFile = new TestFSDataInputStream[1];
    try {
      new RCFile.Reader(fs, path, conf) {
        // this method is called by the RCFile.Reader constructor, overwritten,
        // so we can access the opened file
        protected FSDataInputStream openFile(FileSystem fs, Path file,
          int bufferSize, long length) throws IOException {
          final InputStream in = super.openFile(fs, file, bufferSize, length);
          openedFile[0] = new TestFSDataInputStream(in);
          return openedFile[0];
        }
      };
      fail("IOException expected.");
    } catch (IOException expected) {
    }
    assertNotNull(path + " should have been opened.", openedFile[0]);
    assertTrue("InputStream for " + path + " should have been closed.",
      openedFile[0].isClosed());
  }

}