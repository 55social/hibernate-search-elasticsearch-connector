/**
 *
 */
package org.hibernate.search.elasticsearch;

import java.util.Collections;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.search.FullTextSession;

/** */
public class TestUtil {

  private static final TestUtil instance = new TestUtil();

  private SearchSessionFactory factory;
  private SessionFactory sessionFactory;

  private TestUtil() {
    Configuration cfg = new Configuration();
    cfg.configure(getClass().getClassLoader().getResource("hibernate.cfg.xml"));
    // We need to create the client after you call the buildSessionFactory.
    List classes;
    try {
      // If you are using spring, it's easier, because you could have the
      // persistence class list within another list.
      classes = Collections.singletonList(
          Class.forName("org.hibernate.search.elasticsearch.MockEntity"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ElasticSearchClientFactory client = new ElasticSearchClientFactory("", 0,
        classes, true, true);
    sessionFactory = cfg.buildSessionFactory();
    factory = new SearchSessionFactory(client);
  }

  /** Retrieves the full text session.
   * @return a new full text sesssion.
   */
  public static FullTextSession fullTextSession() {
    return new ElasticSearchFullTextSession(getSession(), instance.factory);
  }

  /** Retrieves a session.
   * @return a hibernate session..
   */
  public static Session getSession() {
    SessionFactory sessionFactory = instance.sessionFactory;
    try {
      return sessionFactory.getCurrentSession();
    } catch (Exception e) {
      return sessionFactory.openSession();
    }
  }

  /** Retrieves the session factory.
   * @return the session factory.
   */
  public static SessionFactory getSessionFactory() {
    return instance.sessionFactory;
  }

}
