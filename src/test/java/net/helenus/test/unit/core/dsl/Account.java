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
package net.helenus.test.unit.core.dsl;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import net.helenus.core.reflect.Drafted;
import net.helenus.mapping.annotation.*;

@Table
public interface Account {

  @PartitionKey
  Long id();

  @ClusteringColumn
  Date time();

  @Index
  @Column("is_active")
  boolean active();

  @Transient
  default Draft draft() {
    return new Draft();
  }

  class Draft implements Drafted<Account> {

    @Override
    public Set<String> mutated() {
      return null;
    }

    @Override
    public Account build() {
      return null;
    }

    @Override
    public Set<String> read() {
      return null;
    }

    @Override
    public Map<String, Object> toMap() {
      return null;
    }
  }
}
