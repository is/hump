package us.yuxin.hump.meta;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import us.yuxin.hump.meta.entity.Piece;

public class TestMetaStore {
  private SessionFactory sessionFactory;

  @Before
  public void setUp() {
    Configuration configuration = new Configuration();
    configuration.addAnnotatedClass(Piece.class);

    ServiceRegistry serviceRegistry = new ServiceRegistryBuilder()
      .applySettings(configuration.getProperties())
      .buildServiceRegistry();
    sessionFactory = configuration
      .buildSessionFactory(serviceRegistry);
  }

  @After
  public void tearDown() {
    if (sessionFactory != null) {
      sessionFactory.close();
    }
  }

  @Test
  public void testBasicUsage() {
    Session session = sessionFactory.openSession();
    session.beginTransaction();

    Piece piece = new Piece();
    piece.id = "hello-world";
    session.save(piece);
    session.getTransaction().commit();
    session.close();

    session = sessionFactory.openSession();
    piece = (Piece)session.get(Piece.class, "hello-world");
    session.evict(piece);
    session.close();
    Assert.assertSame("hello-world", piece.id);
  }
}
