package org.hibernate.search.elasticsearch;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.NumberUtils;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;

import org.elasticsearch.client.Client;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import org.hibernate.HibernateException;
import org.hibernate.search.engine.SearchFactoryImplementor;

import org.hibernate.search.query.engine.impl.EntityInfoImpl;
import org.hibernate.search.query.engine.impl.HSQueryImpl;

import org.hibernate.search.query.engine.spi.EntityInfo;

import org.elasticsearch.index.query.QueryBuilder;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The query implementation, this is where the magic lives!.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticSearchHSQueryImpl extends HSQueryImpl {

  /** The class logger.*/
  private static Logger log = LoggerFactory.getLogger(
      ElasticSearchHSQueryImpl.class);

  /** Max results per query.*/
  private static final int MAX_RESULTS = 200;

  /** The elastic search client, it's never null.*/
  private final Client client;

  /** The sort, can be null.*/
  private Sort sort;

  /** Sets the max results.
   * TODO in the original hibernate search the max is unlimited.
   * is this correct?
   */
  private int maxResults = MAX_RESULTS;

  /** The constructor.
   * @param factory the session factory implementor, cannot be null.
   * @param elasticsearchClient the elastic-search client, cannot be null.
   */
  public ElasticSearchHSQueryImpl(final SearchFactoryImplementor factory,
      final Client elasticsearchClient) {
    super((SearchFactoryImplementor) factory);
    Validate.notNull(elasticsearchClient, "The client cannot be null");
    client = elasticsearchClient;
  }

  /** {@inheritDoc}.
   *
   * This method performs the query to elastic-search.
   */
  @Override
  public List<EntityInfo> queryEntityInfos() {

    log.trace("Entering queryEntityInfos");

    int targetedEntitiesSize = getTargetedEntities().size();
    String[] types = new String[targetedEntitiesSize];

    for (int i = 0; i < targetedEntitiesSize; i++) {
      types[i] = getTargetedEntities().get(i).getName();
    }

    // [waabox] how we handle multiple index here?????.
    Class<?> entity = getTargetedEntities().get(0);
    String indexName = SearchUtils.getIndexName(entity);

    SearchRequestBuilder searchRequest = client.prepareSearch(indexName);

    ElasticsearchQueryBuilder builder;
    builder = new ElasticsearchQueryBuilder(getLuceneQuery(), entity);
    QueryBuilder queryBuilder = builder.build();
    searchRequest.setQuery(queryBuilder);
    searchRequest.setTypes(types);

    log.debug("Sending query: {} to index: {}", queryBuilder, indexName);

    if (sort != null) {
      SortField[] sortFields = sort.getSort();
      for(SortField sortField : sortFields) {
        FieldSortBuilder sortBuilder;
        String name = sortField.getField();
        sortBuilder = new FieldSortBuilder(name + "." + name + "_raw");
        if (sortField.getReverse()) {
          sortBuilder.order(SortOrder.DESC);
        } else {
          sortBuilder.order(SortOrder.ASC);
        }
        searchRequest.addSort(sortBuilder);
      }
    }
    searchRequest.setSize(maxResults);

    SearchResponse response = searchRequest.execute().actionGet();

    List<EntityInfo> entityInfos = new LinkedList<EntityInfo>();

    ShardSearchFailure[] failures = response.getShardFailures();

    if (failures.length > 0) {
      throw new HibernateException(failures[0].reason());
    }

    for (SearchHit hit : response.getHits().getHits()) {
      Class<?> type = SearchUtils.getClassByName(hit.getType());
      Serializable id = createId(hit.getId(), type);
      String idName = SearchUtils.getIdName(type);
      entityInfos.add(new EntityInfoImpl(type, idName, id, null));
    }

    log.trace("Leaving queryEntityInfos");

    return entityInfos;
  }

  /** Creates an id based on the given string-id.
   * @param id the id to transform.
   * @param type the type of the mapped class.
   * @return the id.
   */
  private Serializable createId(final String id, final Class<?> type) {
    Class<?> idType = SearchUtils.getIdType(type);

    if (idType.equals(String.class)) {
      return id;
    }

    // TODO [waabox] add more cases here.

    if (NumberUtils.isNumber(id)) {
      // Should I take care of another kind of ids?
      return Long.parseLong(id);
    } else {
      return id;
    }
  }

  /** {@inheritDoc}.
   * Sets the sort to this instance.
   *
   * @param theSort the sort to set.
   *
   * @return this query.
   */
  public HSQuery sort(final Sort theSort) {
    super.sort(theSort);
    sort = theSort;
    return this;
  }

  /** Sets the max results.
   * @param max the max results.
   * @return the instance.
   */
  public HSQuery maxResults(final Integer max) {
    super.maxResults(max);
    maxResults = max;
    return this;
  }

}
