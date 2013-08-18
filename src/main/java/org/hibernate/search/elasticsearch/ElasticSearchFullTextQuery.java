package org.hibernate.search.elasticsearch;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.hibernate.Criteria;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Query;
import org.hibernate.QueryTimeoutException;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.engine.SessionImplementor;
import org.hibernate.engine.query.ParameterMetadata;
import org.hibernate.impl.AbstractQueryImpl;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.query.DatabaseRetrievalMethod;
import org.hibernate.search.query.ObjectLookupMethod;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.query.engine.spi.FacetManager;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.query.engine.spi.TimeoutManager;
import org.hibernate.search.query.hibernate.impl.IteratorImpl;
import org.hibernate.search.query.hibernate.impl.Loader;
import org.hibernate.search.query.hibernate.impl.ObjectLoaderBuilder;
import org.hibernate.search.query.hibernate.impl.ObjectsInitializer;
import org.hibernate.search.query.hibernate.impl.ProjectionLoader;
import org.hibernate.search.query.hibernate.impl.ScrollableResultsImpl;
import org.hibernate.transform.ResultTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Elastic search full text query implementation.
 *
 * Note: We extend this class because we need to configure the query,
 * so the idea is to delegate everything to our HSQuery.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 *
 */
public class ElasticSearchFullTextQuery extends AbstractQueryImpl implements
    FullTextQuery {

  /** The class logger.*/
  private static Logger log = LoggerFactory.getLogger(
      ElasticSearchFullTextQuery.class);

  /** The Hibernate criteria.*/
  private Criteria criteria;

  /** The result transformer.*/
  private ResultTransformer resultTransformer;

  /** The fetch size.*/
  private int fetchSize = 1;

  /** The lookup method.*/
  private ObjectLookupMethod lookupMethod = ObjectLookupMethod.SKIP;

  /** The strategy to retrieve object when Hibernate populates results
   * from Elastic-Search.
   * Default Query.
   */
  private DatabaseRetrievalMethod retrievalMethod =
      DatabaseRetrievalMethod.QUERY;

  /** The full text query.*/
  private final HSQuery fulltextQuery;

  /** The timeout exception factory.*/
  private static final TimeoutExceptionFactory EXCEPTION_FACTORY =
      new TimeoutExceptionFactory() {
    /** {@inheritDoc}.*/
    public RuntimeException createTimeoutException(final String message,
        final org.apache.lucene.search.Query luceneQuery) {
      return new QueryTimeoutException(message, (SQLException) null,
          luceneQuery.toString());
    }
  };

  /** The null loader.*/
  private static final Loader NO_LOADER = new Loader() {
    /** {@inheritDoc}.*/
    public void init(final Session session,
        final SearchFactoryImplementor searchFactoryImplementor,
        final ObjectsInitializer objectsInitializer,
        final TimeoutManager timeoutManager) {
      log.trace("Entering init");
      log.trace("Leaving init");
    }
    /** {@inheritDoc}.*/
    public Object load(final EntityInfo entityInfo) {
      throw new UnsupportedOperationException("loader should not be used");
    }
    /** {@inheritDoc}.*/
    public Object loadWithoutTiming(final EntityInfo entityInfo) {
      throw new UnsupportedOperationException("loader should not be used");
    }
    /** {@inheritDoc}.*/
    public List<?> load(final EntityInfo... entityInfos) {
      throw new UnsupportedOperationException("loader should not be used");
    }
  };

  /** Constructs a new instance of the Elastic Search Query.
   *
   * @param query the Lucene query, cannot be null.
   * @param classes the classes to search, cannot be null.
   * @param session the Hibernate session, cannot be null.
   * @param factory the search session factory, cannot be null.
   */
  public ElasticSearchFullTextQuery(final org.apache.lucene.search.Query query,
      final Class<?>[] classes, final SessionImplementor session,
      final SearchSessionFactory factory) {

    super(query.toString(), null, session, new ParameterMetadata(null, null));

    Validate.notNull(query, "The lucene query cannot be null");
    Validate.notNull(classes, "The classes cannot be null");
    Validate.notNull(session, "The session cannot be null");
    Validate.notNull(factory, "The factory cannot be null");

    fulltextQuery = factory.getSearchFactory(session).createHSQuery();
    fulltextQuery.luceneQuery(query);
    fulltextQuery.timeoutExceptionFactory(EXCEPTION_FACTORY);
    fulltextQuery.targetedEntities(Arrays.asList(classes));
  }

  /**{@inheritDoc}. */
  public FullTextQuery setSort(final Sort sort) {
    fulltextQuery.sort(sort);
    return this;
  }

  /**{@inheritDoc}.*/
  public FullTextQuery setFilter(final Filter filter) {
    fulltextQuery.filter(filter);
    return this;
  }

  /** Return an iterator on the results. Retrieve the object one by one
   * (initialize it during the next() operation)
   * @return the iterator.
   */
  @SuppressWarnings("rawtypes")
  public Iterator iterate() {
    log.trace("Entering iterate");
    // implement an iterator which keep the id/class for each hit and get the
    // object on demand
    // cause I can't keep the searcher and hence the hit opened. I don't have
    // any hook to know when the
    // user stops using it
    // scrollable is better in this area
    fulltextQuery.getTimeoutManager().start();
    List<EntityInfo> entityInfos = fulltextQuery.queryEntityInfos();
    // stop timeout manager, the iterator pace is in the user's hands
    fulltextQuery.getTimeoutManager().stop();
    // TODO is this noloader optimization really needed?
    Iterator<Object> iterator;
    if (entityInfos.size() == 0) {
      return new IteratorImpl(entityInfos, NO_LOADER);
    } else {
      Loader loader = getLoader();
      iterator = new IteratorImpl(entityInfos, loader);
    }
    fulltextQuery.getTimeoutManager().stop();
    log.trace("Leaving iterate");
    return iterator;
  }

  /** Decide which object loader to use depending on the targeted entities. If
   * there is only a single entity targeted a <code>QueryLoader</code> can be
   * used which will only execute a single query to load the entities. If more
   * than one entity is targeted a <code>MultiClassesQueryLoader</code> must be
   * used. We also have to consider whether projections or <code>Criteria</code>
   * are used.
   *
   * @return The loader instance to use to load the results of the query.
   */
  @SuppressWarnings("deprecation")
  private Loader getLoader() {
    ObjectLoaderBuilder loaderBuilder = new ObjectLoaderBuilder();
    loaderBuilder.criteria(criteria);
    loaderBuilder.targetedEntities(fulltextQuery.getTargetedEntities());
    loaderBuilder.indexedTargetedEntities(
        fulltextQuery.getIndexedTargetedEntities());
    loaderBuilder.session(session);
    loaderBuilder.searchFactory(fulltextQuery.getSearchFactoryImplementor());
    loaderBuilder.timeoutManager(fulltextQuery.getTimeoutManager());
    loaderBuilder.lookupMethod(lookupMethod).retrievalMethod(retrievalMethod);

    if (fulltextQuery.getProjectedFields() != null) {
      return getProjectionLoader(loaderBuilder);
    } else {
      return loaderBuilder.buildLoader();
    }
  }

  /** Retrieves the projection loaders.
   * @param loaderBuilder the builder.
   * @return the loader.
   */
  @SuppressWarnings("deprecation")
  private Loader getProjectionLoader(final ObjectLoaderBuilder loaderBuilder) {
    ProjectionLoader loader = new ProjectionLoader();
    loader.init((Session) session, fulltextQuery.getSearchFactoryImplementor(),
        resultTransformer, loaderBuilder, fulltextQuery.getProjectedFields(),
        fulltextQuery.getTimeoutManager());
    return loader;
  }

  /** {@inheritDoc}.*/
  public ScrollableResults scroll() {
    log.trace("Entering scroll");
    // keep the searcher open until the resultset is closed
    fulltextQuery.getTimeoutManager().start();
    DocumentExtractor extractor = fulltextQuery.queryDocumentExtractor();
    // stop timeout manager, the iterator pace is in the user's hands
    fulltextQuery.getTimeoutManager().stop();
    Loader loader = getLoader();
    ScrollableResults result;
    result = new ScrollableResultsImpl(fetchSize, extractor, loader, session);
    log.trace("Leaving scroll");
    return result;
  }

  /** {@inheritDoc}.*/
  public ScrollableResults scroll(final ScrollMode scrollMode) {
    // TODO think about this scrollmode
    return scroll();
  }

  /** {@inheritDoc}.*/
  public List<?> list() {
    log.trace("Entering list");
    fulltextQuery.getTimeoutManager().start();
    final List<EntityInfo> entityInfos = fulltextQuery.queryEntityInfos();
    Loader loader = getLoader();
    EntityInfo[] infos = new EntityInfo[entityInfos.size()];
    List<?> list = loader.load(entityInfos.toArray(infos));
    // no need to timeoutManager.isTimedOut from this point, we don't do
    // anything intensive
    if (resultTransformer == null || loader instanceof ProjectionLoader) {
      log.debug("stay consistent with transformTuple which can only be "
          + "executed during a projection, nothing to do here.");
    } else {
      list = resultTransformer.transformList(list);
    }
    fulltextQuery.getTimeoutManager().stop();
    log.trace("Leaving list");
    return list;
  }

  /** {@inheritDoc}.*/
  public Explanation explain(final int documentId) {
    return fulltextQuery.explain(documentId);
  }

  /** {@inheritDoc}.*/
  public int getResultSize() {
    return fulltextQuery.queryResultSize();
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setCriteriaQuery(final Criteria theCriteria) {
    criteria = theCriteria;
    return this;
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setProjection(final String... fields) {
    fulltextQuery.projection(fields);
    return this;
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setFirstResult(final int firstResult) {
    fulltextQuery.firstResult(firstResult);
    return this;
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setMaxResults(final int maxResults) {
    fulltextQuery.maxResults(maxResults);
    return this;
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setFetchSize(final int theFetchSize) {
    super.setFetchSize(theFetchSize);
    if (theFetchSize <= 0) {
      throw new IllegalArgumentException(
          "'fetch size' parameter less than or equals to 0");
    }
    fetchSize = theFetchSize;
    return this;
  }

  /** {@inheritDoc}.*/
  public Query setLockOptions(final LockOptions lockOptions) {
    throw new UnsupportedOperationException(
        "Lock options are not implemented in Hibernate search queries");
  }

  /** {@inheritDoc}.*/
  @Override
  public FullTextQuery setResultTransformer(
      final ResultTransformer transformer) {
    super.setResultTransformer(transformer);
    resultTransformer = transformer;
    return this;
  }

  /** {@inheritDoc}.*/
  @SuppressWarnings("unchecked")
  public <T> T unwrap(final Class<T> type) {
    if (type == org.apache.lucene.search.Query.class) {
      return (T) fulltextQuery.getLuceneQuery();
    }
    throw new IllegalArgumentException("Cannot unwrap " + type.getName());
  }

  /** {@inheritDoc}.*/
  public LockOptions getLockOptions() {
    throw new UnsupportedOperationException(
        "Lock options are not implemented in Hibernate search queries");
  }

  /** {@inheritDoc}.*/
  public int executeUpdate() {
    throw new UnsupportedOperationException(
        "executeUpdate is not supported in Hibernate search queries");
  }

  /** {@inheritDoc}.*/
  public Query setLockMode(final String alias, final LockMode lockMode) {
    throw new UnsupportedOperationException(
        "Lock options are not implemented in Hibernate search queries");
  }

  /** Retrieves the lock modes. Method not supported within this implementation.
   * @return nothing, just raise an exception.
   */
  @SuppressWarnings("rawtypes")
  protected Map getLockModes() {
    throw new UnsupportedOperationException(
        "Lock options are not implemented in Hibernate search queries");
  }

  /** {@inheritDoc}.*/
  public FullTextFilter enableFullTextFilter(final String name) {
    return fulltextQuery.enableFullTextFilter(name);
  }

  /** {@inheritDoc}.*/
  public void disableFullTextFilter(final String name) {
    fulltextQuery.disableFullTextFilter(name);
  }

  /** {@inheritDoc}.*/
  public FacetManager getFacetManager() {
    return fulltextQuery.getFacetManager();
  }

  /** {@inheritDoc}.*/
  @Override
  public FullTextQuery setTimeout(final int timeout) {
    return setTimeout(timeout, TimeUnit.SECONDS);
  }

  /** {@inheritDoc}.*/
  public FullTextQuery setTimeout(final long timeout,
      final TimeUnit timeUnit) {
    super.setTimeout((int) timeUnit.toSeconds(timeout));
    fulltextQuery.getTimeoutManager().setTimeout(timeout, timeUnit);
    fulltextQuery.getTimeoutManager().raiseExceptionOnTimeout();
    return this;
  }

  /** {@inheritDoc}.*/
  public FullTextQuery limitExecutionTimeTo(final long timeout,
      final TimeUnit timeUnit) {
    fulltextQuery.getTimeoutManager().setTimeout(timeout, timeUnit);
    fulltextQuery.getTimeoutManager().limitFetchingOnTimeout();
    return this;
  }

  /** {@inheritDoc}.*/
  public boolean hasPartialResults() {
    return fulltextQuery.getTimeoutManager().hasPartialResults();
  }

  /** {@inheritDoc}.*/
  public FullTextQuery initializeObjectsWith(
      final ObjectLookupMethod theLookupMethod,
      final DatabaseRetrievalMethod theRetrievalMethod) {
    lookupMethod = theLookupMethod;
    retrievalMethod = theRetrievalMethod;
    return this;
  }
}
