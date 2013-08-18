package org.hibernate.search.elasticsearch;

import org.apache.commons.lang.Validate;
import org.elasticsearch.client.Client;
import org.hibernate.search.impl.ImmutableSearchFactory;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.internals.SearchFactoryState;

/** The search factory.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticSearchImmutableSearchFactory
  extends ImmutableSearchFactory {

  /** The elastic search client, it's never null.*/
  private final Client client;

  /** Creates a new instance of the search factory.
   * @param state the search factory state.
   * @param elasticsearchClient the elastic search client.
   */
  public ElasticSearchImmutableSearchFactory(final SearchFactoryState state,
      final Client elasticsearchClient) {
    super(state);
    Validate.notNull(elasticsearchClient,
        "The elastic search client cannot be null");
    client = elasticsearchClient;
  }

  /** {@inheritDoc}.*/
  @Override
  public HSQuery createHSQuery() {
    return new ElasticSearchHSQueryImpl(this, client);
  }
}
