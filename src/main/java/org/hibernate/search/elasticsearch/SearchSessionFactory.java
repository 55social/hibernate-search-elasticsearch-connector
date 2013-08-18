package org.hibernate.search.elasticsearch;

import org.apache.commons.lang.Validate;
import org.elasticsearch.client.Client;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.event.PostInsertEventListener;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.event.FullTextIndexEventListener;
import org.hibernate.search.spi.internals.SearchFactoryState;

/** Helper class to get a full text session out of a regular session.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class SearchSessionFactory {

  /** Default index name.*/
  public static final String DEFAULT_INDEX_NAME = "esdefaultindex";

  /** The elastic-search client, it's never null.*/
  private final Client client;

  /** Creates a new instance of the search session factory.
   * @param clientFactory the elasticsearch client factory, cannot be null.
   */
  public SearchSessionFactory(final ElasticSearchClientFactory clientFactory) {
    Validate.notNull(clientFactory, "The client factory cannot be null");
    client = clientFactory.get();
  }

  /** Retrieves a new full text session if the given session it's not a
   * full text session.
   * @param session the hibernate session, cannot be null.
   * @return the full text session.
   */
  public FullTextSession getFullTextSession(final Session session) {
    Validate.notNull(session, "The hibernate session cannot be null");

    if (!ElasticSearchClientFactory.isActive()) {
      return Search.getFullTextSession(session);
    }

    if (session instanceof ElasticSearchFullTextSession) {
      return (FullTextSession) session;
    } else {
      return new ElasticSearchFullTextSession(session, this);
    }
  }

  /** Creates the new search factory implementor.
   * @param session the Hibernate session.
   * @return the search factory implementor.
   */
  public SearchFactoryImplementor getSearchFactory(
      final SessionImplementor session) {
    SearchFactoryState state = null;
    PostInsertEventListener[] listeners;
    listeners = session.getListeners().getPostInsertEventListeners();
    for (PostInsertEventListener candidate :listeners) {
      if (candidate instanceof FullTextIndexEventListener) {
        FullTextIndexEventListener listener;
        listener = (FullTextIndexEventListener) candidate;
        state = (SearchFactoryState) listener.getSearchFactoryImplementor();
        break;
      }
    }

    if (state == null) {
      throw new HibernateException(
          "Hibernate SearchSessionFactory Event listeners not configured,"
              + "please check the reference documentation and the "
              + "application's hibernate.cfg.xml");
    }

    return new ElasticSearchImmutableSearchFactory(state, client);
  }

  /** Retrieves the search factory implementor.
   * @param session the session.
   * @return the search factory implementor.
   */
  public SearchFactoryImplementor getSearchFactory(
      final Session session) {
    return getSearchFactory((SessionImplementor) session);
  }

  /** Retrieves the elasticsearch client.
   * @return the client, never null.
   */
  Client getClient() {
    return client;
  }

}
