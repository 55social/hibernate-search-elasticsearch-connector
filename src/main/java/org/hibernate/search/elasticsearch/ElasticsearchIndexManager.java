package org.hibernate.search.elasticsearch;

import java.util.List;

import org.apache.commons.lang.Validate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices
  .IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices
  .IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that holds operations that allows to create or delete indices.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
final class ElasticsearchIndexManager {

  /** The class logger.*/
  private static Logger log = LoggerFactory.getLogger(
      ElasticsearchIndexManager.class);

  /** Utility class constructor.*/
  private ElasticsearchIndexManager() {
  }

  /** Creates the given index list.
   * @param classes the classes to index, cannot be null.
   * @param client the elastic search client.
   */
  public static void createIndex(final List<Class<?>> classes,
      final Client client) {
    Validate.notNull(classes, "The list of classes cannot be null");
    Validate.notNull(client, "The client cannot be null");
    for (Class<?> theClass : classes) {
      createIndex(theClass, client);
    }
  }

  /** Creates an index for the given indexed class.
   * @param indexedClass the indexed class, cannot be null.
   * @param client the elastic-search client, cannot be null.
   */
  public static void createIndex(final Class<?> indexedClass,
      final Client client) {
    Validate.notNull(indexedClass, "The indexed class cannot be null");
    Validate.notNull(client, "The client cannot be null");

    try {

      IndicesAdminClient indicesAdmin = client.admin().indices();

      if (indexedClass.isAnnotationPresent(Indexed.class)) {

        Indexed indexed = indexedClass.getAnnotation(Indexed.class);
        String indexName = indexed.index();

        IndicesExistsRequest request;
        request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response;
        response = indicesAdmin.exists(request).get();

        if (!response.isExists()) {
          java.lang.reflect.Field[] fields = indexedClass.getDeclaredFields();

          String theType = indexedClass.getName();

          XContentBuilder builder = XContentFactory.jsonBuilder();
          builder.startObject();
          builder.startObject(theType).startObject("properties");

          for (java.lang.reflect.Field field : fields) {
            if (field.isAnnotationPresent(Field.class)) {
              String dataType = getType(field);
              builder.
              startObject(field.getName()).
                field("type").value("multi_field").
                  startObject("fields").
                    startObject(field.getName()).
                      field("index", "analyzed").
                      field("store", "yes").
                      field("type", dataType).
                    endObject().
                    startObject(field.getName() + "_raw").
                      field("index", "not_analyzed").
                      field("store", "yes").
                      field("type", dataType).
                    endObject().
                  endObject().
              endObject();
            }
          }

          builder.endObject().endObject().endObject();

          log.debug("mapping: {}", builder.prettyPrint().string());

          // lets create the index.
          CreateIndexRequest createRequest;
          createRequest = new CreateIndexRequest(indexName);
          createRequest.mapping(theType, builder);

          CreateIndexResponse createResponse;
          createResponse = indicesAdmin.create(createRequest).actionGet();

          if (!createResponse.acknowledged()) {
            throw new RuntimeException("not acknowledged the put operation");
          }
        }
        waitFor(indexName, client);
      }

    } catch (Exception e) {
      throw new RuntimeException("Cannot create the index", e);
    }

    log.debug("finished the index creation.");
  }

  /** Retrieves the string type representation for the given field.
   * @param field the field to extract the data-type.
   * @return the string representation.
   */
  private static String getType(final java.lang.reflect.Field field) {
    Class<?> type = field.getType();
    String name = type.getName();

    if (type.isPrimitive()) {
      if ("int".equals(name)) {
        return "integer";
      } else {
        return name;
      }
    }

    if (type == java.util.Date.class) {
      return "date";

    } else if (Number.class.isAssignableFrom(type)) {
      return name.substring(name.lastIndexOf('.') + 1).toLowerCase();

    } else if (type.isAssignableFrom(Boolean.class)) {
      return "boolean";
    }

    return "string";
  }

  /** Wait until the given index finish its initialization.
   * @param indexName the index, cannot be null.
   * @param client the elasticsearch client, cannot be null.
   */
  private static void waitFor(final String indexName, final Client client) {
    Validate.notNull(indexName, "The index name cannot be null");
    Validate.notNull(client, "The client cannot be null");

    log.debug("waiting...");
    ClusterHealthRequest healthRequest = new ClusterHealthRequest(
        indexName);
    healthRequest.waitForYellowStatus();
    client.admin().cluster().health(healthRequest).actionGet();
    log.debug("done!, index named:" + indexName + " created");
  }

  /** Deletes the given index.
   * @param index the index to delete, cannot be null.
   * @param client the elasticsearch client, cannot be null.
   */
  public static void deleteIndex(final Class<?> indexedClass,
      final Client client) {

    Validate.notNull(indexedClass, "The indexed classes cannot be null");
    Validate.notNull(client, "The client cannot be null");

    String index = "";
    if (indexedClass.isAnnotationPresent(Indexed.class)) {
      Indexed indexed = indexedClass.getAnnotation(Indexed.class);
      index = indexed.index();
    }
    try {
      DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);
      client.admin().indices().delete(deleteRequest).actionGet();
    } catch (Exception e) {
      log.debug("indices not found, creating new one named:" + index);
    }
  }

  /** Recreates the given index.
   * @param indexedClass the index class to re-create, cannot be null.
   * @param client the elasticsearch client, cannot be null.
   */
  public static void recreateIndex(final Class<?> indexedClass,
      final Client client) {
    try {
      deleteIndex(indexedClass, client);
    } catch (Exception e) {
      log.warn("Cannot delete the index", e);
    }
    createIndex(indexedClass, client);
  }

}
