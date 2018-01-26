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

import com.datastax.driver.core.DataType.Name;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;
import net.helenus.mapping.annotation.Types;

@Table
public interface AccountWithCollections {

  @PartitionKey
  long id();

  @Types.Set(Name.TEXT)
  Set<String> aliases();

  @Types.List(Name.TEXT)
  List<String> name();

  @Types.Map(key = Name.TEXT, value = Name.TEXT)
  Map<String, String> properties();
}
