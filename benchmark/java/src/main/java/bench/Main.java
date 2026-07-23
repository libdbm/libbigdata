package bench;

import java.nio.file.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * parquet-mr read/write benchmark.
 *
 * read:  Main read  <dataset> <path>        <reps>
 * write: Main write <dataset> <csv>  <codec> <reps>
 *
 * Emits one line per repetition:
 *   RESULT parquet-mr <op> <dataset> <codec> <ms> <rows> <bytes> <ints> <floats>
 */
public final class Main {
  static final Configuration CONF = new Configuration();

  public static void main(String[] args) throws Exception {
    CONF.set("fs.defaultFS", "file:///");
    String op = args[0];
    if (op.equals("read")) {
      read(args[1], args[2], Integer.parseInt(args[3]));
    } else if (op.equals("write")) {
      write(args[1], args[2], args[3], Integer.parseInt(args[4]));
    } else {
      System.err.println("unknown op: " + op);
      System.exit(2);
    }
  }

  static void read(String dataset, String path, int reps) throws Exception {
    long bytes = Files.size(Paths.get(path));
    for (int r = 0; r < reps; r++) {
      long t0 = System.nanoTime();
      long ints = 0;
      double floats = 0;
      long rows = 0;
      try (ParquetReader<Group> reader =
          ParquetReader.builder(new GroupReadSupport(), new Path(path))
              .withConf(CONF).build()) {
        Group g;
        while ((g = reader.read()) != null) {
          rows++;
          if (dataset.equals("flights")) {
            ints += longAt(g, "DEP_DELAY") + longAt(g, "ARR_DELAY")
                + longAt(g, "AIR_TIME") + longAt(g, "DISTANCE")
                + (present(g, "FL_DATE") ? 1 : 0);
            floats += floatAt(g, "DEP_TIME") + floatAt(g, "ARR_TIME");
          } else {
            ints += g.getLong("id", 0) + g.getLong("quantity", 0)
                + g.getLong("ts", 0)
                + g.getBinary("category", 0).length()
                + g.getBinary("name", 0).length()
                + (g.getBoolean("active", 0) ? 1 : 0)
                + (present(g, "score") ? 1 : 0);
            floats += g.getDouble("amount", 0)
                + (present(g, "score") ? g.getDouble("score", 0) : 0);
          }
        }
        if (dataset.equals("flights")) ints += rows; // + COUNT(*)
      }
      emit("read", dataset, "none", t0, rows, bytes, ints, floats);
    }
  }

  static void write(String dataset, String csv, String codec, int reps)
      throws Exception {
    CompressionCodecName cc = CompressionCodecName.valueOf(codec.toUpperCase());
    String out = "out_parquet-mr_" + codec + ".parquet";
    MessageType schema = MessageTypeParser.parseMessageType(
        "message gen {"
        + " required int64 id;"
        + " required double amount;"
        + " required binary category (UTF8);"
        + " required binary name (UTF8);"
        + " required int64 quantity;"
        + " required boolean active;"
        + " required int64 ts;"
        + " optional double score;"
        + " }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    for (int r = 0; r < reps; r++) {
      List<String> lines = Files.readAllLines(Paths.get(csv));
      long t0 = System.nanoTime();
      Files.deleteIfExists(Paths.get(out));
      long rows = 0;
      CONF.set("parquet.example.schema", schema.toString());
      try (ParquetWriter<Group> writer =
          ExampleParquetWriter.builder(new Path(out))
              .withConf(CONF).withType(schema)
              .withCompressionCodec(cc).build()) {
        for (int i = 1; i < lines.size(); i++) { // skip header
          String[] f = lines.get(i).split(",", -1); // keep trailing empty score
          if (f.length < 8) continue;
          Group g = factory.newGroup()
              .append("id", Long.parseLong(f[0]))
              .append("amount", Double.parseDouble(f[1]))
              .append("category", f[2])
              .append("name", f[3])
              .append("quantity", Long.parseLong(f[4]))
              .append("active", Boolean.parseBoolean(f[5]))
              .append("ts", Long.parseLong(f[6]));
          if (!f[7].isEmpty()) g.append("score", Double.parseDouble(f[7]));
          writer.write(g);
          rows++;
        }
      }
      long sz = Files.size(Paths.get(out));
      emit("write", dataset, codec, t0, rows, sz, 0, 0);
    }
  }

  static long longAt(Group g, String name) {
    int idx = g.getType().getFieldIndex(name);
    return g.getFieldRepetitionCount(idx) > 0 ? g.getInteger(idx, 0) : 0;
  }

  static double floatAt(Group g, String name) {
    int idx = g.getType().getFieldIndex(name);
    return g.getFieldRepetitionCount(idx) > 0 ? g.getFloat(idx, 0) : 0;
  }

  static boolean present(Group g, String name) {
    return g.getFieldRepetitionCount(g.getType().getFieldIndex(name)) > 0;
  }

  static void emit(String op, String dataset, String codec, long t0,
      long rows, long bytes, long ints, double floats) {
    double ms = (System.nanoTime() - t0) / 1e6;
    System.out.printf("RESULT parquet-mr %s %s %s %.3f %d %d %d %s%n",
        op, dataset, codec, ms, rows, bytes, ints,
        floats == Math.rint(floats)
            ? String.valueOf((long) floats) : String.valueOf(floats));
  }
}
