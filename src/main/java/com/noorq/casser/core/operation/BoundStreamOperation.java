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
package com.noorq.casser.core.operation;

import java.util.stream.Stream;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public final class BoundStreamOperation<E> extends AbstractStreamOperation<E, BoundStreamOperation<E>> {

	private final BoundStatement boundStatement;
	private final AbstractStreamOperation<E, ?> delegate;
	
	public BoundStreamOperation(BoundStatement boundStatement, AbstractStreamOperation<E, ?> operation) {
		super(operation.sessionOps);
		this.boundStatement = boundStatement;
		this.delegate = operation;
	}
	
	@Override
	public Stream<E> transform(ResultSet resultSet) {
		return delegate.transform(resultSet);
	}

	@Override
	public BuiltStatement buildStatement() {
		return null;
	}
	
}
