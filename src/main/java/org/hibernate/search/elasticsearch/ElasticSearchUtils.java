package org.hibernate.search.elasticsearch;

import java.lang.reflect.Field;

import javax.persistence.Id;

import org.apache.commons.lang.Validate;
import org.hibernate.search.annotations.Indexed;

/** Helper class for elastic-search.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
final class ElasticSearchUtils {

  /** Utility class constructor.*/
  private ElasticSearchUtils() {
  }

  /** Retrieves the index name defined by the given class.
   * @param klass the class to retrieve the index name, cannot be null.
   * @return the string with the index name.
   */
  public static String getIndexName(final Class<?> klass) {
    Indexed indexed = klass.getAnnotation(Indexed.class);
    String name = indexed.index();
    if ("".equals(name)) {
      return SearchSessionFactory.DEFAULT_INDEX_NAME;
    }
    return name;
  }

  /** Retrieves a Class instance based on the fully class name.
   * @param type the fully class name, cannot be null.
   * @return the class.
   */
  public static Class<?> getClassByName(final String type) {
    try {
      return Class.forName(type);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /** Extract the name of the declared id.
   * @param type the class type.
   * @return the string name for the id.
   */
  public static String getIdName(final Class<?> type) {
    return getFieldId(type).getName();
  }

  /** Extract the type of the declared id.
   * @param type the class type.
   * @return the type of the id.
   */
  public static Class<?> getIdType(final Class<?> type) {
    return getFieldId(type).getType();
  }

  /** Retrieves the field with the annotation Id.
   * @param type class type, cannot be null.
   * @return the field.
   */
  private static Field getFieldId(final Class<?> type) {
    Validate.notNull(type, "The type cannot be null");
    Class<?> currentType = type;
    do {
      for (Field field : currentType.getDeclaredFields()) {
        if (field.isAnnotationPresent(Id.class)) {
          return field;
        }
      }
      currentType = currentType.getSuperclass();
    } while (currentType != Object.class);

    throw new RuntimeException("The type:" + type.getName()
        + " did not define the id property");
  }

}
