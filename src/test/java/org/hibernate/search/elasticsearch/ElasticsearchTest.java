package org.hibernate.search.elasticsearch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

/** Unit test for elastic-search hibernate-search integration.*/
public class ElasticsearchTest {

  private MockEntityRepository repository;

  @Before public void setUp() throws Exception {
    repository = new MockEntityRepository(TestUtil.getSessionFactory());
  }


  @Test public void search_untokenizedSorted() throws Exception {
    repository.save(new MockEntity("waabox", "a geek"));
    repository.save(new MockEntity("waab", "the second original"));
    repository.save(new MockEntity("waabuz", "the second original"));
    repository.save(new MockEntity("mirabelli", "a nerd"));
    repository.save(new MockEntity("graña", "just like gandalf :p"));
    repository.save(new MockEntity("mario roman", "geiiiiiii"));

    List<MockEntity> entities =  repository.searchLikeName("w");
    assertThat(entities.size(), is(3));
    assertThat(entities.get(0).getName(), is("waab"));
  }

  @Test public void search_tokenizedSorted() {

    repository.save(new MockEntity("waabo", "a chinesee copy of waabox"));

    List<MockEntity> entities = repository.searchLikeDescription("chinesee");
    assertThat(entities.size(), is(1));
  }

}
