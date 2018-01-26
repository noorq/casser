/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.test.integration.core.tuplecollection;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TupleCollectionTest extends AbstractEmbeddedCassandraTest {

  static Book book;

  static HelenusSession session;

  @BeforeClass
  public static void beforeTest() {
    session = Helenus.init(getSession()).showCql().add(Book.class).autoCreateDrop().get();
    book = Helenus.dsl(Book.class, session.getMetadata());
  }

  @Test
  public void test() {
    System.out.println(book);
  }

  public static final class AuthorImpl implements Author {

    String name;
    String city;

    AuthorImpl(String name, String city) {
      this.name = name;
      this.city = city;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String city() {
      return city;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((city == null) ? 0 : city.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      AuthorImpl other = (AuthorImpl) obj;
      if (city == null) {
        if (other.city != null) return false;
      } else if (!city.equals(other.city)) return false;
      if (name == null) {
        if (other.name != null) return false;
      } else if (!name.equals(other.name)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "AuthorImpl [name=" + name + ", city=" + city + "]";
    }
  }

  public static final class SectionImpl implements Section {

    String title;
    int page;

    SectionImpl(String title, int page) {
      this.title = title;
      this.page = page;
    }

    @Override
    public String title() {
      return title;
    }

    @Override
    public int page() {
      return page;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + page;
      result = prime * result + ((title == null) ? 0 : title.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      SectionImpl other = (SectionImpl) obj;
      if (page != other.page) return false;
      if (title == null) {
        if (other.title != null) return false;
      } else if (!title.equals(other.title)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "SectionImpl [title=" + title + ", page=" + page + "]";
    }
  }
}
