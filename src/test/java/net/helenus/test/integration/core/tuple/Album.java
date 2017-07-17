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
package net.helenus.test.integration.core.tuple;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;
import net.helenus.mapping.annotation.Types;

@Table
public interface Album {

	@PartitionKey(ordinal=1)
	int id();

	AlbumInformation info();

	@Types.Tuple({DataType.Name.TEXT, DataType.Name.TEXT})
	@Column(ordinal=1)
	TupleValue infoNoMapping();

}
