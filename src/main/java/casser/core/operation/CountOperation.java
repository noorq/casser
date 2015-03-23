/*
 *      Copyright (C) 2015 Noorq Inc.
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
package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public class CountOperation extends AbstractObjectOperation<Long, CountOperation> {

	public CountOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	@Override
	public BuiltStatement buildStatement() {
		return null;
	}
	
	@Override
	public Long transform(ResultSet resultSet) {
		return resultSet.one().getLong(0);
	}
	
	
}
