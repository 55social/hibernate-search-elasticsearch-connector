package org.hibernate.search.elasticsearch;

import org.apache.commons.lang.Validate;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.Client;
import org.hibernate.Session;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.SearchFactory;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.impl.FullTextSessionImpl;

/** Elastic search Hibernate client.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticSearchFullTextSession extends FullTextSessionImpl {

  /** The serial version.*/
  private static final long serialVersionUID = 1L;

  /** The hibernate session, it's never null.*/
  private final Session session;

  /** The search factory implementor, it's never null.*/
  private final SearchFactoryImplementor searchFactory;

  /** The search session factory, it's never null. */
  private final SearchSessionFactory searchSessionFactory;

  /** Creates the Elastic search full text session.
   * @param hibernateSession the current hibernate session, cannot be null.
   * @param factory the search session factory, cannot be null.
   */
  public ElasticSearchFullTextSession(final Session hibernateSession,
      final SearchSessionFactory factory) {
    super(hibernateSession);
    Validate.notNull(hibernateSession, "The hibernate session cannot be null");
    Validate.notNull(factory, "The hibernate search factory cannot be null");
    session = hibernateSession;
    searchFactory = factory.getSearchFactory(session);
    searchSessionFactory = factory;
  }

  /** {@inheritDoc}.
   * Retrieves our search factory implementor, just because this class
   * creates the HSQuery.
   */
  @Override
  public SearchFactory getSearchFactory() {
    return searchFactory;
  }

  /** {@inheritDoc}.*/
  @Override
  @SuppressWarnings("rawtypes")
  public FullTextQuery createFullTextQuery(final Query query,
      final Class... entities) {
    return new ElasticSearchFullTextQuery(query, entities,
        (SessionImplementor) session, searchSessionFactory);
  }

  /** {@inheritDoc}.*/
  @Override
  public MassIndexer createIndexer(final Class<?>... types) {
    Client client = searchSessionFactory.getClient();
    for (Class<?> indexedClass : types) {
      ElasticsearchIndexManager.recreateIndex(indexedClass, client);
    }
    MassIndexer indexer = super.createIndexer(types);
    indexer.progressMonitor(ElasticsearchBatchBackendQueue.createMonitor());
    return indexer;
  }
}
