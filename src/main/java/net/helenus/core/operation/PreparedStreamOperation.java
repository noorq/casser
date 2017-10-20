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
package net.helenus.core.operation;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

public final class PreparedStreamOperation<E> {

	private final PreparedStatement preparedStatement;
	private final AbstractStreamOperation<E, ?> operation;

	public PreparedStreamOperation(PreparedStatement statement, AbstractStreamOperation<E, ?> operation) {
		this.preparedStatement = statement;
		this.operation = operation;
	}

	public PreparedStatement getPreparedStatement() {
		return preparedStatement;
	}

	public BoundStreamOperation<E> bind(Object... params) {
		BoundStatement boundStatement = preparedStatement.bind(params);
		return new BoundStreamOperation<E>(boundStatement, operation);
	}

	@Override
	public String toString() {
		return preparedStatement.getQueryString();
	}
}
