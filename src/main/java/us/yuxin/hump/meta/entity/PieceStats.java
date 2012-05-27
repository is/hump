package us.yuxin.hump.meta.entity;


import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "piecestats")
public class PieceStats {
  @Id
  @GeneratedValue
  public int id;
  public String category;
  public String key;
  public long count;
  public long rows;
}
