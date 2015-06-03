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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Future;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.util.concurrent.ListenableFuture;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.support.CasserException;
import com.noorq.casser.support.Fun;
import com.noorq.casser.support.Scala;

public abstract class AbstractStatementOperation<E, O extends AbstractStatementOperation<E, O>> {

	final Logger logger = LoggerFactory.getLogger(getClass());

	protected final AbstractSessionOperations sessionOps;
	
	public abstract Statement buildStatement();
	
	protected boolean showValues = true;
	private ConsistencyLevel consistencyLevel;
	private ConsistencyLevel serialConsistencyLevel;
	private RetryPolicy retryPolicy;
	private boolean enableTracing = false;
	private long[] defaultTimestamp = null;
	private int[] fetchSize = null;
	
	public AbstractStatementOperation(AbstractSessionOperations sessionOperations) {
		this.sessionOps = sessionOperations;
	}
	
	public O showValues(boolean enabled) {
		this.showValues = enabled;
		return (O) this;
	}
	
	public O defaultTimestamp(long timestamp) {
		this.defaultTimestamp = new long[1];
		this.defaultTimestamp[0] = timestamp;
		return (O) this;
	}
	
	public O retryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return (O) this;
	}

	public O defaultRetryPolicy() {
		this.retryPolicy = DefaultRetryPolicy.INSTANCE;
		return (O) this;
	}

	public O downgradingConsistencyRetryPolicy() {
		this.retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
		return (O) this;
	}

	public O fallthroughRetryPolicy() {
		this.retryPolicy = FallthroughRetryPolicy.INSTANCE;
		return (O) this;
	}

	public O consistency(ConsistencyLevel level) {
		this.consistencyLevel = level;
		return (O) this;
	}

	public O consistencyAny() {
		this.consistencyLevel = ConsistencyLevel.ANY;
		return (O) this;
	}

	public O consistencyOne() {
		this.consistencyLevel = ConsistencyLevel.ONE;
		return (O) this;
	}

	public O consistencyQuorum() {
		this.consistencyLevel = ConsistencyLevel.QUORUM;
		return (O) this;
	}

	public O consistencyAll() {
		this.consistencyLevel = ConsistencyLevel.ALL;
		return (O) this;
	}

	public O serialConsistency(ConsistencyLevel level) {
		this.serialConsistencyLevel = level;
		return (O) this;
	}

	public O serialConsistencyAny() {
		this.serialConsistencyLevel = ConsistencyLevel.ANY;
		return (O) this;
	}

	public O serialConsistencyOne() {
		this.serialConsistencyLevel = ConsistencyLevel.ONE;
		return (O) this;
	}

	public O serialConsistencyQuorum() {
		this.serialConsistencyLevel = ConsistencyLevel.QUORUM;
		return (O) this;
	}

	public O serialConsistencyAll() {
		this.serialConsistencyLevel = ConsistencyLevel.ALL;
		return (O) this;
	}	
	
	public O disableTracing() {
		this.enableTracing = false;
		return (O) this;
	}

	public O enableTracing() {
		this.enableTracing = true;
		return (O) this;
	}

	public O tracing(boolean enable) {
		this.enableTracing = enable;
		return (O) this;
	}

	public O fetchSize(int fetchSize) {
		this.fetchSize = new int[1];
		this.fetchSize[0] = fetchSize;
		return (O) this;
	}
	
	protected Statement options(Statement statement) {
		
		if (defaultTimestamp != null) {
			statement.setDefaultTimestamp(defaultTimestamp[0]);
		}
		
		if (consistencyLevel != null) {
			statement.setConsistencyLevel(consistencyLevel);
		}
		
		if (serialConsistencyLevel != null) {
			statement.setSerialConsistencyLevel(serialConsistencyLevel);
		}
		
		if (retryPolicy != null) {
			statement.setRetryPolicy(retryPolicy);
		}
		
		if (enableTracing) {
			statement.enableTracing();
		}
		else {
			statement.disableTracing();
		}
		
		if (fetchSize != null) {
			statement.setFetchSize(fetchSize[0]);
		}
		
		return statement;
	}

	public Statement statement() {
		return buildStatement();
	}
	
	public String cql() {
		Statement statement = buildStatement(); 
		if (statement instanceof BuiltStatement) {
			BuiltStatement buildStatement = (BuiltStatement) statement;
			return buildStatement.setForceNoValues(true).getQueryString();
		}
		else {
			return statement.toString();
		}
	}
	
	public PreparedStatement prepareStatement() {
		
		Statement statement = buildStatement();
		
		if (statement instanceof RegularStatement) {
			
			RegularStatement regularStatement = (RegularStatement) statement;
			
			
			
			return sessionOps.prepare(regularStatement);
		}
		
		throw new CasserException("only RegularStatements can be prepared");
	}

	public ListenableFuture<PreparedStatement> prepareStatementAsync() {
		
		Statement statement = buildStatement();
		
		if (statement instanceof RegularStatement) {
			
			RegularStatement regularStatement = (RegularStatement) statement;
			
			return sessionOps.prepareAsync(regularStatement);
			
		}
		
		throw new CasserException("only RegularStatements can be prepared");
	}
	
	public Future<PreparedStatement> prepareStatementFuture() {
		return Scala.asFuture(prepareStatementAsync());
	}

	public <A> Future<Fun.Tuple2<PreparedStatement, A>> prepareStatementFuture(A a) {
		return Scala.asFuture(prepareStatementAsync(), a);
	}

	public <A, B> Future<Fun.Tuple3<PreparedStatement, A, B>> prepareStatementFuture(A a, B b) {
		return Scala.asFuture(prepareStatementAsync(), a, b);
	}

	public <A, B, C> Future<Fun.Tuple4<PreparedStatement, A, B, C>> prepareStatementFuture(A a, B b, C c) {
		return Scala.asFuture(prepareStatementAsync(), a, b, c);
	}

	public <A, B, C, D> Future<Fun.Tuple5<PreparedStatement, A, B, C, D>> prepareStatementFuture(A a, B b, C c, D d) {
		return Scala.asFuture(prepareStatementAsync(), a, b, c, d);
	}

}
