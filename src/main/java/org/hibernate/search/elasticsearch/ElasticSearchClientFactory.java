package org.hibernate.search.elasticsearch;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.Validate;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/** Creates elastic search client instances.
 *
 * Note The Hibernate session factory should be dependent of this bean.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticSearchClientFactory {

  /** The class logger.*/
  private static Logger log = LoggerFactory.getLogger(
      ElasticSearchClientFactory.class);

  /** Elastic-search directory. */
  private static final String HOME_FOLDER = "target/elasticsearch";

  /** The client factory, it's never null. */
  private static ElasticSearchClientFactory factory;

  /** The elastic-search host, it's never null. */
  private final String host;

  /** The elastic-search port, it's never null. */
  private final int port;

  /** This flag checks if the application is running on debug mode.*/
  private final boolean runLocalInstance;

  /** The elastic-search client, it's never null. */
  private Client client;

  /** The local node, can be null if <code>runLocalInstance</code> is false.*/
  private Node node;

  /** The list of persistent classes mapped within Hibernate, never null.*/
  private final List<Class<?>> persistentClasses;

  /** Checks if it's running locally. */
  private boolean localInstance;

  /** This flag enables or disable the elasticsearch. */
  private boolean enabled = true;

  /** Creates a new instance of the elastic search client.
   * @param elasticSearchHost host, cannot be null.
   * @param elasticSearchPort port.
   * @param hibernatePersistentClasses the hibernate mapped classes,
   * cannot be null.
   * @param local debug point.
   * @param active checks if elastic search will be active or not.
   */
  public ElasticSearchClientFactory(final String elasticSearchHost,
      final int elasticSearchPort,
      final List<Class<?>> hibernatePersistentClasses, final boolean local,
      final boolean active) {

    Validate.notNull(elasticSearchHost, "The host cannot be null");
    Validate.notNull(hibernatePersistentClasses,
        "The persistance classes cannot be null");

    host = elasticSearchHost;
    port = elasticSearchPort;
    runLocalInstance = local;
    persistentClasses = hibernatePersistentClasses;
    localInstance = local;
    enabled = active;

    if (enabled) {

      log.debug("Creating elastic search client");

      if (runLocalInstance) {

        Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));

        log.debug("Elastic search is running in development-mode");

        String folderName = HOME_FOLDER;

        ImmutableSettings.Builder settingBuilder;
        settingBuilder = ImmutableSettings.settingsBuilder();
        settingBuilder.put("path.home", folderName);
        settingBuilder.put("node.data", true);
        settingBuilder.put("node.local", true);
        settingBuilder.put("http.port", 0);

        Settings setttings = settingBuilder.build();
        node = NodeBuilder.nodeBuilder().settings(setttings).build();

        node.start();
        client = node.client();

        for (Class<?> theClass : persistentClasses) {
          ElasticsearchIndexManager.deleteIndex(theClass, client);
        }

      } else {
        log.debug("Elastic search is running in production-mode");
        InetSocketTransportAddress transport;
        transport = new InetSocketTransportAddress(host, port);
        client = new TransportClient().addTransportAddress(transport);
      }

      ElasticsearchIndexManager.createIndex(persistentClasses, client);

    } else {
      log.debug("Elastic search it's not active.");
    }

    factory = this;
  }

  /** Retrieves the Elastic-SearchUtils client.
   * @return the Elastic search client, never null.
   */
  public Client get() {
    return factory.client;
  }

  /** Retrieves the client just one time, then clear the internal reference.
   * @return the client.
   */
  static Client getClient() {
    log.debug("Retrieving the elasticsearch client for internal use");
    return factory.get();
  }

  /** Destroys the client factory.*/
  public static void destroy() {
    if (factory != null) {
      factory.shutshow();
    }
  }

  /** Destroys the client factory.*/
  public void shutshow() {
    if (factory != null) {
      factory.client.close();
      if (factory.node != null) {
        if (!factory.node.isClosed()) {
          factory.node.close();
        }
      }
      factory = null;
    }
  }

  /** Retrieves the current instance of the factory.
   * @return the current instance.
   */
  static ElasticSearchClientFactory instance() {
    return factory;
  }

  /** Checks if it's running as a single local instance.
   * @return true if it's running as local instance.
   */
  boolean isLocalInstance() {
    return localInstance;
  }

  /** Checks if elastic search is active or not.
   * @return true if it's active.
   */
  static boolean isActive() {
    return instance().enabled;
  }

  /** The shutdown hook. */
  private static final class ShutdownHook extends Thread {

    /** The factory.*/
    private final ElasticSearchClientFactory clientFactory;

    /** Creates a new instance of the ShutdownHook.
     * @param theFactory the elasticsearch client factory.
     */
    private ShutdownHook(final ElasticSearchClientFactory theClientFactory) {
      clientFactory = theClientFactory;
    }

    /** {@inheritDoc}. */
    @Override
    public void run() {
      super.run();
      clientFactory.shutshow();
    }
  }
}
