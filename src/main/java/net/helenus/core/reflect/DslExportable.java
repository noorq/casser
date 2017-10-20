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
package net.helenus.core.reflect;

import com.datastax.driver.core.Metadata;

import net.helenus.mapping.HelenusEntity;

public interface DslExportable {

	public static final String GET_ENTITY_METHOD = "getHelenusMappingEntity";
	public static final String GET_PARENT_METHOD = "getParentDslHelenusPropertyNode";
	public static final String SET_METADATA_METHOD = "setCassandraMetadataForHelenusSesion";

	HelenusEntity getHelenusMappingEntity();

	HelenusPropertyNode getParentDslHelenusPropertyNode();

	void setCassandraMetadataForHelenusSesion(Metadata metadata);
}
