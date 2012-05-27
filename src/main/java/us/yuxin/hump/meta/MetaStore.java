package us.yuxin.hump.meta;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hibernate.transform.ResultTransformer;
import us.yuxin.hump.meta.entity.Piece;
import us.yuxin.hump.meta.entity.PieceStats;

public class MetaStore {
  String dbUrl;
  SessionFactory sessionFactory;
  Session session;

  public MetaStore() {
  }


  public void create(String url) {
    open(url, true);
  }


  public void open(String url) {
    open(url, false);
  }


  /**
   * Open existed metastore.
   *
   * @param url Datasource JDBC URL.
   */
  private void open(String url, boolean init) {
    this.dbUrl = url;
    Configuration configuration = new Configuration();
    configuration.setProperty("hibernate.connection.url", url);

    if (init)
      configuration.setProperty("hibernate.hbm2ddl.auto", "create");
    else
      configuration.setProperty("hibernate.hbm2ddl.auto", "validate");

    if (url.contains("jdbc:h2:")) {
      configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
    }

    configuration.addAnnotatedClass(Piece.class);
    configuration.addAnnotatedClass(PieceStats.class);
    ServiceRegistry serviceRegistry = new ServiceRegistryBuilder()
      .applySettings(configuration.getProperties())
      .buildServiceRegistry();


    sessionFactory = configuration.buildSessionFactory(serviceRegistry);
    session = sessionFactory.openSession();
  }


  public void close() {
    if (session != null) {
      session.close();
    }

    if (sessionFactory != null) {
      sessionFactory.close();
    }
  }


  public void importSummaryLog(BufferedReader br) throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    long beginTS = System.currentTimeMillis();

    int flashCount = 0;
    int bigCount = 0;
    int dotCount = 0;
    session.beginTransaction();

    while (true) {
      String line = br.readLine();
      if (line == null)
        break;

      JsonNode node = mapper.readValue(line.trim(), JsonNode.class);
      Piece piece = new Piece();
      if (node.get("id") == null)
        continue;

      piece.loadFromJson(node);
      if (!piece.state.equals("SKIP")) {
        session.saveOrUpdate(piece);
        ++flashCount;
        ++bigCount;
        ++dotCount;
        if (dotCount >= 5000) {
          System.out.write('.');
          System.out.flush();
          dotCount = 0;
        }

        if (flashCount > 500) {
          session.flush();
          session.clear();
          flashCount = 0;
        }
      }
    }
    session.getTransaction().commit();

    System.out.println('.');
    System.out.format("Update %d pieces in %.2f seconds\n", bigCount,
      (System.currentTimeMillis() - beginTS) / 1000f);
  }


  public void importSummaryLog(String filename) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    importSummaryLog(br);
    br.close();
  }


  public void updatePieceStatistic() {
    String query = "SELECT '%s', " +
      "p.%s, count(p), sum(p.rows) " +
      "FROM Piece p GROUP BY p.%s ORDER BY p.%s";

    String columns[] = new String[]{"name", "label1", "label2"};

    session.createSQLQuery("TRUNCATE TABLE piecestats").executeUpdate();
    session.beginTransaction();

    for (String column : columns) {
      String hql = String.format(query, column, column, column, column);
      List res = session.createQuery(hql).setResultTransformer(new ResultTransformer() {
        @Override
        public Object transformTuple(Object[] tuple, String[] aliases) {
          PieceStats ps = new PieceStats();
          ps.category = (String) tuple[0];
          ps.key = (String) tuple[1];
          ps.count = (Long) tuple[2];
          ps.rows = (Long) tuple[3];
          // System.out.format("TT: ca:%s, key:%s, c:%d, r:%d\n",
          //   ps.category, ps.key, ps.count, ps.rows);
          return ps;
        }

        @Override
        public List transformList(List collection) {
          // System.out.println("transformList:" + collection.size());
          return collection;
        }
      }).list();

      for (Object ops : res) {
        PieceStats ps = (PieceStats) ops;
        session.save(ps);
      }
    }

    session.getTransaction().commit();
  }


  public String[] getPieceStatisticByColumn(String column) {
    List<String> res = new ArrayList<String>();

    for (Object o : session.createQuery(
      String.format("select ps from PieceStats ps WHERE ps.category = '%s'", column)).list()) {
      PieceStats ps = (PieceStats) o;
      res.add(String.format("%s:%d/%d", ps.key, ps.rows, ps.count));
    }

    return res.toArray(new String[res.size()]);
  }


  public List<Piece> getPieces(String query) {
    List<Piece> pieces = new LinkedList<Piece>();

    List res = session.createQuery(query).list();

    for (Object o : res) {
      session.evict(o);
      pieces.add((Piece) o);
    }

    return pieces;
  }
}
