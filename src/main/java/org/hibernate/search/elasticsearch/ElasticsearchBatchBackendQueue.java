package org.hibernate.search.elasticsearch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.Client;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.batchlucene.LuceneBatchBackend;
import org.hibernate.search.batchindexing.MassIndexerProgressMonitor;
import org.hibernate.search.spi.WorkerBuildContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performs bach operations to the index.
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticsearchBatchBackendQueue extends LuceneBatchBackend {

  /** The class logger. */
  private static Logger log = LoggerFactory.getLogger(
      ElasticsearchBatchBackendQueue.class);

  /** Seconds to wait for the next iteration of lucene works. */
  private static final long TIMER_DELAY = TimeUnit.SECONDS.toMillis(3);

  /** The number of elements to queue within the TIMER_DELAY. */
  private static final int NUMBER_OF_WORKS_TO_QUEUE = 100;

  /** The private instance. */
  private static ElasticsearchBatchBackendQueue instance;

  /** The elastic search client. */
  private Client client;

  /** The lucene works queue. */
  private List<LuceneWork> works = new ArrayList<LuceneWork>();

  /** The timer. */
  private static Timer timer;

  /** {@inheritDoc}. */
  @Override
  public void initialize(final Properties cfg,
      final MassIndexerProgressMonitor monitor,
      final WorkerBuildContext context) {
    if (ElasticSearchClientFactory.isActive()) {
      super.initialize(cfg, monitor, context);
      client = ElasticSearchClientFactory.getClient();
    } else {
      super.initialize(cfg, monitor, context);
    }
    instance = this;
  }

  /** {@inheritDoc}. */
  public void enqueueAsyncWork(final LuceneWork work) {
    if (ElasticSearchClientFactory.isActive()) {
      synchronized (works) {
        works.add(work);
        if (works.size() >= NUMBER_OF_WORKS_TO_QUEUE) {
          new BachTask().run();
        }
      }
    } else {
      try {
        super.enqueueAsyncWork(work);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Starts the current queue.*/
  static void start() {
    if (timer != null) {
      stop();
    }
    timer = new Timer("ElasticsearchBatchBackendQueue-timer", true);
    timer.schedule(new BachTask(), TIMER_DELAY);
  }

  /** Stops the current queue.*/
  static void stop() {
    new BachTask().run();
    timer.cancel();
  }

  /** Creates the indexing monitor.
   * @return the indexing monitor.
   */
  static MassIndexerProgressMonitor createMonitor() {
    return new Monitor();
  }

  /** The task to execute. */
  public static class BachTask extends TimerTask {
    /** {@inheritDoc}.*/
    @Override
    public void run() {
      List<LuceneWork> theWorks = new LinkedList<LuceneWork>(instance.works);
      new ElasticSearchBackendQueue(theWorks, instance.client, true).run();
      instance.works.clear();
    }
  }

  /** The indexer monitor. */
  private static final class Monitor implements MassIndexerProgressMonitor {

    /** The number of indexed documents.*/
    private long total = 0;

    /** Creates a new monitor.*/
    private Monitor() {
      log.debug("indexer started");
      ElasticsearchBatchBackendQueue.start();
    }

    /** {@inheritDoc}. */
    public void documentsAdded(final long increment) {
      log.debug("documents added: {}", increment);
    }

    /** {@inheritDoc}. */
    public void documentsBuilt(final int number) {
      log.debug("documents built: {}", number);
    }

    /** {@inheritDoc}. */
    public void entitiesLoaded(final int size) {
      log.debug("entities loaded: {}", size);
    }

    /** {@inheritDoc}. */
    public void addToTotalCount(final long count) {
      total += count;
      log.debug("indexed: {}", total);
    }

    /** {@inheritDoc}. */
    public void indexingCompleted() {
      log.debug("indexing completed");
      ElasticsearchBatchBackendQueue.stop();
    }
  }

}
