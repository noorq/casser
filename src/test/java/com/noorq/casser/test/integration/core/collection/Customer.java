/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.test.integration.core.collection;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.DataType.Name;
import com.noorq.casser.mapping.annotation.Column;
import com.noorq.casser.mapping.annotation.PartitionKey;
import com.noorq.casser.mapping.annotation.Table;
import com.noorq.casser.mapping.annotation.Types;

@Table
public interface Customer {

	@PartitionKey(ordinal=0)
	UUID id();
	
	@Types.Set(Name.TEXT)
	@Column(ordinal=1)
	Set<String> aliases();
	
	@Types.List(Name.TEXT)
	@Column(ordinal=2)
	List<String> name();
	
	@Types.Map(key=Name.TEXT, value=Name.TEXT)
	@Column(ordinal=3)
	Map<String, String> properties();

}
