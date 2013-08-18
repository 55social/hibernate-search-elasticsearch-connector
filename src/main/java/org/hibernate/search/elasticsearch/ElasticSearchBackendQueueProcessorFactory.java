package org.hibernate.search.elasticsearch;

import java.util.List;
import java.util.Properties;

import org.elasticsearch.client.Client;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.lucene
  .LuceneBackendQueueProcessorFactory;
import org.hibernate.search.spi.WorkerBuildContext;

/** Factory for the elastic search backend queue processor.
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticSearchBackendQueueProcessorFactory
  extends LuceneBackendQueueProcessorFactory {

  /** The elastic search client, can be null if it's not invoked the method
   * initialize.
   */
  private Client client;

  /** {@inheritDoc}. */
  public void initialize(final Properties hibernateSearchProperties,
      final WorkerBuildContext context) {
    if (ElasticSearchClientFactory.isActive()) {
      client = ElasticSearchClientFactory.getClient();
    } else {
      super.initialize(hibernateSearchProperties, context);
    }
  }

  /** {@inheritDoc}. */
  public Runnable getProcessor(final List<LuceneWork> queue) {
    if (ElasticSearchClientFactory.isActive()) {
      boolean local;
      local = ElasticSearchClientFactory.instance().isLocalInstance();
      return new ElasticSearchBackendQueue(queue, client, local);
    } else {
      return super.getProcessor(queue);
    }
  }

  /** {@inheritDoc}. */
  public void close() {
    if (ElasticSearchClientFactory.isActive()) {
      ElasticSearchClientFactory.destroy();
    } else {
      super.close();
    }
  }
}
