package org.hibernate.search.elasticsearch;

import static org.easymock.EasyMock.*;

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.hibernate.search.backend.AddLuceneWork;
import org.hibernate.search.backend.DeleteLuceneWork;
import org.hibernate.search.backend.LuceneWork;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchBackendQueueTest {

  private ElasticSearchBackendQueue queue;
  private Client client;
  private List<LuceneWork> luceneWorks = new LinkedList<LuceneWork>();
  private BulkRequestBuilder requestBuilder;

  @Before
  public void setUp() throws Exception {
    client = createMock(Client.class);
    requestBuilder = createMock(BulkRequestBuilder.class);
    expect(requestBuilder.execute()).andReturn(null);
    expect(client.prepareBulk()).andReturn(requestBuilder);
  }

  @Test public void run_handleNewDocument() {
    luceneWorks.clear();
    LuceneWork addLuceneWork = createMock(AddLuceneWork.class);
    Document document = new Document();
    IndexRequestBuilder builder = createMock(IndexRequestBuilder.class);

    expect(addLuceneWork.getEntityClass()).andReturn(MockEntity.class).times(2);
    expect(addLuceneWork.getIdInString()).andReturn("1");
    expect(addLuceneWork.getDocument()).andReturn(document);

    expect(client.prepareIndex("mock_entity", MockEntity.class.getName(), "1"))
      .andReturn(builder);
    expect(builder.setSource(isA(XContentBuilder.class))).andReturn(builder);
    expect(requestBuilder.add(builder)).andReturn(requestBuilder);

    replay(client, addLuceneWork, builder, requestBuilder);
    luceneWorks.add(addLuceneWork);

    queue = new ElasticSearchBackendQueue(luceneWorks, client, false);

    queue.run();
    verify(client, addLuceneWork, builder, requestBuilder);
  }

  @Test public void run_handleDelete() {
    luceneWorks.clear();
    LuceneWork deleteLuceneWork = createMock(DeleteLuceneWork.class);
    DeleteRequestBuilder builder = createMock(DeleteRequestBuilder.class);

    expect(deleteLuceneWork.getEntityClass()).andReturn(MockEntity.class)
      .times(2);
    expect(deleteLuceneWork.getIdInString()).andReturn("1");

    luceneWorks.add(deleteLuceneWork);

    expect(client.prepareDelete("mock_entity", MockEntity.class.getName(), "1")
        ).andReturn(builder);

    replay(client, deleteLuceneWork);

    queue = new ElasticSearchBackendQueue(luceneWorks, client, false);

    queue.run();

    verify(client, deleteLuceneWork);
  }

}
