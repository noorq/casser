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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.util.concurrent.ListenableFuture;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.support.CasserException;

public abstract class AbstractStatementOperation<E, O extends AbstractStatementOperation<E, O>> {

	protected final AbstractSessionOperations sessionOps;
	
	public abstract BuiltStatement buildStatement();
	
	public AbstractStatementOperation(AbstractSessionOperations sessionOperations) {
		this.sessionOps = sessionOperations;
	}
	
	public String cql() {
		return buildStatement().setForceNoValues(true).getQueryString();
	}
	
	public PreparedStatement prepareStatement() {
		
		BuiltStatement builtStatement = buildStatement();
		
		if (builtStatement instanceof RegularStatement) {
			
			RegularStatement statement = (RegularStatement) builtStatement;
			
			return sessionOps.prepare(statement);
		}
		
		throw new CasserException("only RegularStatements can be prepared");
	}

	public ListenableFuture<PreparedStatement> prepareStatementAsync() {
		
		BuiltStatement builtStatement = buildStatement();
		
		if (builtStatement instanceof RegularStatement) {
			
			RegularStatement statement = (RegularStatement) builtStatement;
			
			return sessionOps.prepareAsync(statement);
			
			/*
			return Futures.transform(preparedStatementFuture, new Function<PreparedStatement, Prepared<O>>() {

				@Override
				public Prepared<O> apply(PreparedStatement preparedStatement) {
					return new Prepared<O>(preparedStatement, _this);
				}
				
			});
			*/
			
		}
		
		throw new CasserException("only RegularStatements can be prepared");
	}
}
