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
package net.helenus.test.integration.core.usertype;

import com.datastax.driver.core.DataType;
import java.util.Set;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.Types;
import net.helenus.mapping.annotation.UDT;

@UDT("address")
public interface Address {

  @Column(ordinal = 0, value = "line_1")
  String street();

  @Column
  String city();

  @Column
  int zip();

  @Column
  String country();

  @Column
  @Types.Set(DataType.Name.TEXT)
  Set<String> phones();
}
