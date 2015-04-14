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
import com.noorq.casser.mapping.annotation.DataTypeName;
import com.noorq.casser.mapping.annotation.column.Column;
import com.noorq.casser.mapping.annotation.column.PartitionKey;
import com.noorq.casser.mapping.annotation.entity.Table;

@Table
public interface Customer {

	@PartitionKey(0)
	UUID id();
	
	@DataTypeName(value = Name.SET, types={Name.TEXT})
	@Column(1)
	Set<String> aliases();
	
	@DataTypeName(value = Name.LIST, types={Name.TEXT})
	@Column(2)
	List<String> name();
	
	@DataTypeName(value = Name.MAP, types={Name.TEXT, Name.TEXT})
	@Column(3)
	Map<String, String> properties();

}
