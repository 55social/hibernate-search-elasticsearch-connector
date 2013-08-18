package org.hibernate.search.elasticsearch;

import java.lang.reflect.Field;

import org.apache.commons.lang.Validate;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.hibernate.search.annotations.Index;

/**
 * This class creates a QueryBuilder for elasticsearch based on a Lucene query.
 *
 * @author waabox (waabox[at]gmail[dot]com)
 */
public class ElasticsearchQueryBuilder {

  /** The lucene query, it's never null. */
  private final Query query;

  /** The entity, it's never null. */
  private final Class<?> entity;

  /** Creates a new instance of the factory.
   * @param luceneQuery
   */
  public ElasticsearchQueryBuilder(final Query luceneQuery,
      final Class<?> targetEntity) {
    Validate.notNull(luceneQuery, "The lucene query cannot be null");
    query = luceneQuery;
    entity = targetEntity;
  }

  /** Retrieves the query builder.
   * @return the query builder, never null.
   */
  public QueryBuilder build() {
    // case for non-query.
    if (query.toString().equals("()")) {
      return QueryBuilders.matchAllQuery();
    }
    // case for queries :D
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      BoolQueryBuilder booleanQueryBuilder = QueryBuilders.boolQuery();
      booleanQuery(booleanQueryBuilder, booleanQuery.getClauses());
      return booleanQueryBuilder;
    } else if (query instanceof WildcardQuery) {
      return wildcard(query);
    } else {
      return QueryBuilders.queryString(query.toString());
    }
  }

  /** Resolves the boolean clauses.
   * @param booleanQuertBuilder the query builder.
   * @param clouses the boolean clauses.
   */
  private void booleanQuery(final BoolQueryBuilder booleanQuertBuilder,
      final BooleanClause[] clouses) {
    for (BooleanClause clause : clouses) {
      if (clause.getQuery() instanceof BooleanQuery) {
        BooleanQuery booleanQuery = (BooleanQuery) clause.getQuery();
        if (booleanQuery.getClauses().length > 0) {
          booleanQuery(booleanQuertBuilder, booleanQuery.getClauses());
        } // { else } should never been here, do I decide to ignore it.
      } else {
        Query query = clause.getQuery();
        // Here you can add lot of types, fuzzy, etc.
        if (query instanceof WildcardQuery) {
          accur(booleanQuertBuilder, clause, wildcard(query));
        } else {
          accur(booleanQuertBuilder, clause,
              QueryBuilders.queryString(query.toString()));
        }
      }
    }
  }

  /** Generates a wildcard based on the given lucene query.
   * @param luceneQuery the lucene query.
   */
  private QueryBuilder wildcard(final Query luceneQuery) {
    Term term = ((WildcardQuery) luceneQuery).getTerm();
    String termField = term.field();
    try {
      Field field = entity.getDeclaredField(termField);
      org.hibernate.search.annotations.Field searchField;
      searchField = field.getAnnotation(
          org.hibernate.search.annotations.Field.class);

      if (searchField.index() == Index.UN_TOKENIZED) {
        // So, if it's not-tokenized, we use the raw value to search.
        return QueryBuilders.wildcardQuery(termField + "_raw", term.text());
      } else {
        return QueryBuilders.queryString(luceneQuery.toString());
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Assign the boolean association within the given boolean query builder.
   * @param booleanQueryBuilder the boolean query builder.
   * @param clause the boolean clause.
   * @param elasticsearchQuery the elasticsearch query.
   */
  private void accur(final BoolQueryBuilder booleanQueryBuilder,
      final BooleanClause clause, final QueryBuilder elasticsearchQuery) {
    switch (clause.getOccur()) {
    case MUST:
      booleanQueryBuilder.must(elasticsearchQuery);
      break;
    case MUST_NOT:
      booleanQueryBuilder.mustNot(elasticsearchQuery);
      break;
    case SHOULD:
      booleanQueryBuilder.should(elasticsearchQuery);
      break;
    default:
      throw new IllegalStateException("Boolean clause must contains accour");
    }
  }

}
