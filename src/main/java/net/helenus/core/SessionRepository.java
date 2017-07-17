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
package net.helenus.core;

import java.util.Collection;

import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableMap;

import net.helenus.mapping.HelenusEntity;

public final class SessionRepository {

	private final ImmutableMap<String, UserType> userTypeMap;

	private final ImmutableMap<Class<?>, HelenusEntity> entityMap;

	public SessionRepository(SessionRepositoryBuilder builder) {

		userTypeMap = ImmutableMap.<String, UserType>builder().putAll(builder.getUserTypeMap()).build();

		entityMap = ImmutableMap.<Class<?>, HelenusEntity>builder().putAll(builder.getEntityMap()).build();
	}

	public UserType findUserType(String name) {
		return userTypeMap.get(name.toLowerCase());
	}

	public Collection<HelenusEntity> entities() {
		return entityMap.values();
	}

}
