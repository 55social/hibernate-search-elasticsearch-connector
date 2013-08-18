package org.hibernate.search.elasticsearch;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.WildcardQuery;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.MassIndexer;

/** This is just for test purposes.*/
public class MockEntityRepository {

  private SessionFactory sessionFactory;

  public MockEntityRepository(final SessionFactory factory) {
    sessionFactory = factory;
  }

  /** Stores the given mock entity.
   * @param entity the entity to store.
   */
  public void save(final MockEntity entity) {
    Session session = sessionFactory.openSession();
    Transaction tx = session.beginTransaction();
    session.saveOrUpdate(entity);
    tx.commit();
    session.flush();
  }

  /** Search mock entities given by its name.
   * @param name the name to search.
   * @return a list of mock entities.
   */
  @SuppressWarnings("unchecked")
  public List<MockEntity> searchLikeName(final String name) {
    FullTextSession fullTextSession = TestUtil.fullTextSession();
    WildcardQuery wilCardQuery = new WildcardQuery(new Term("name",
        name.trim() + "*"));
    FullTextQuery query = fullTextSession
        .createFullTextQuery(wilCardQuery, MockEntity.class);
    query.setSort(new Sort(new SortField("name", SortField.STRING)));
    return query.list();
  }

  /** Search mock entities given by its name.
   * @param name the name to search.
   * @return a list of mock entities.
   */
  @SuppressWarnings("unchecked")
  public List<MockEntity> searchLikeDescription(final String name) {
    FullTextSession fullTextSession = TestUtil.fullTextSession();
    WildcardQuery wilCardQuery = new WildcardQuery(new Term("description",
        name.trim() + "*"));
    FullTextQuery query = fullTextSession
        .createFullTextQuery(wilCardQuery, MockEntity.class);
    query.setSort(new Sort(new SortField("description", SortField.STRING)));
    return query.list();
  }

  /** Regenerates the Lucene index of the given list of classes.
  *
  * If one of the classes does not have index attached nothing is done.
  *
  * @param theClasses The list of classes which indexes have to be
  *   regenerated, cannot be null.
  */
  public void reindex(final String... theClasses) {
    Session session = sessionFactory.openSession();
    Transaction tx = session.beginTransaction();
    FullTextSession fts = TestUtil.fullTextSession();
    session.setDefaultReadOnly(false);
    try {
      for (String className : theClasses) {
        MassIndexer indexer = fts.createIndexer(Class.forName(className));
        indexer.batchSizeToLoadObjects(1)
        .cacheMode(CacheMode.IGNORE)
        .threadsToLoadObjects(1)
        .threadsForIndexWriter(1)
        .threadsForSubsequentFetching(1).startAndWait();
      }
    } catch (Exception error) {
      throw new RuntimeException(error);
    }
    tx.commit();
  }

}
