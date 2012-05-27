package us.yuxin.hump.meta.entity;

import javax.jdo.annotations.Index;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.JsonNode;

@Entity
@Table(name = "piece")
public class Piece {
  @Id
  public String id;

  @Index
  public String name;
  public String schema;
  public String category;

  @Index
  public String label1;
  @Index
  public String label2;
  public String label3;
  public String tags;
  public String state;

  public String target;
  public String cext;
  public int rows;
  public long size;

  @Column(length = 1024)
  public String columns;

  @Column(length = 1024)
  public String hivetypes;

  @Column(length = 1024)
  public String sqltypes;

  public Timestamp created;
  public Timestamp lastUpdate;


  public void loadFromJson(JsonNode node) {
    id = node.get("id").getTextValue();
    String tokens[] = Iterables.toArray(Splitter.on('.').split(id), String.class);
    schema = "log";

    category = tokens[0];
    name = tokens[1];
    label1 = tokens[2];
    label2 = tokens[3];

    state = node.get("status").getTextValue();
    target = node.get("target").getTextValue();
    if (node.get("cext") != null) {
      cext = node.get("cext").getTextValue();
    }

    if (state.equals("OK")) {
      rows = node.get("rows").getIntValue();
      size = node.get("fileBytes").getLongValue();
      columns = node.get("columns").getTextValue();
      hivetypes = node.get("columnTypes").getTextValue();

      created = new Timestamp(System.currentTimeMillis());
      lastUpdate = new Timestamp(System.currentTimeMillis());
    }
  }
}
