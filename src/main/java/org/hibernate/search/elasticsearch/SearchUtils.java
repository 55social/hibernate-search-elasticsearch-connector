package org.hibernate.search.elasticsearch;

import java.lang.reflect.Field;

import javax.persistence.Id;

import org.hibernate.search.annotations.Indexed;

/** Helper class for elastic-search.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
final class SearchUtils {

  /** Default index name.*/
  public static final String DEFAULT_INDEX_NAME = "55social";

  /** Utility class constructor.*/
  private SearchUtils() {
  }

  /** Retrieves the index name defined by the given class.
   * @param klass the class to retrieve the index name, cannot be null.
   * @return the string with the index name.
   */
  public static String getIndexName(final Class<?> klass) {
    Indexed indexed = klass.getAnnotation(Indexed.class);
    String name = indexed.index();
    if ("".equals(name)) {
      return DEFAULT_INDEX_NAME;
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
   * @param type tye class type.
   * @return the field.
   * TODO [waabox] cache this result.
   */
  private static Field getFieldId(final Class<?> type) {
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
