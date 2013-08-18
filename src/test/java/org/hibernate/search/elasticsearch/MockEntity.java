package org.hibernate.search.elasticsearch;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/** Mock entity to test the elastic-search hibernate-search implementation.*/
@Entity
@Table(name = "mock_entity")
@Indexed(index = "mock_entity")
public class MockEntity {

  /** The id.*/
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column(name = "id", nullable = false)
  private long id = 0;

  /** The name.*/
  @Column(name = "name")
  @Field(store = Store.YES, index = Index.TOKENIZED)
  private String name;

  /** The description.*/
  @Column(name = "description")
  @Field(store = Store.YES, index = Index.TOKENIZED)
  private String description;

  /** The gender.*/
  @Column(name = "gender")
  @Field(store = Store.YES, index = Index.UN_TOKENIZED)
  private String gender;

  /** The age.*/
  @Column(name = "age")
  @Field(store = Store.YES)
  private Integer age;

  /** The year.*/
  @Column(name = "year")
  @Field(store = Store.YES)
  private int year;

  /** The date.*/
  @Column(name = "date")
  @Field(store = Store.YES)
  private Date date;

  /** ORM constructor.*/
  MockEntity() {
  }

  /** Creates a new instance of the mock-entity.
   * @param theName the name.
   * @param theDescription the description.
   */
  public MockEntity(final String theName, final String theDescription) {
    name = theName;
    description = theDescription;
    gender = "male";
    age = 1;
    year = 1;
    date = new Date();
  }

  /** Retrieves the id.
   * @return the id.
   */
  public long getId() {
    return id;
  }

  /** Retrieves the name.
   * @return the name.
   */
  public String getName() {
    return name;
  }

  /** Retrieves the description.
   * @return the description.
   */
  public String getDescription() {
    return description;
  }

}
