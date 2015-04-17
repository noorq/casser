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
package com.noorq.casser.test.integration.core.compound;


import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.DataType.Name;
import com.noorq.casser.mapping.annotation.ClusteringColumn;
import com.noorq.casser.mapping.annotation.Column;
import com.noorq.casser.mapping.annotation.DataTypeName;
import com.noorq.casser.mapping.annotation.PartitionKey;
import com.noorq.casser.mapping.annotation.Table;

@Table
public interface Timeline {

	@PartitionKey(ordinal=0)
	UUID userId();
	
	@ClusteringColumn(ordinal=1)
	@DataTypeName(Name.TIMEUUID)
	Date timestamp();
	
	@Column(ordinal=2)
	String text();
	
}
