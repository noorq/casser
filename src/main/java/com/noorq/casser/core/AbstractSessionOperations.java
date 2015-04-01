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
package com.noorq.casser.core;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.util.concurrent.ListenableFuture;
import com.noorq.casser.mapping.value.ColumnValuePreparer;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.support.CasserException;

public abstract class AbstractSessionOperations {

	final Logger logger = LoggerFactory.getLogger(getClass());
	
	abstract public Session currentSession();
	
	abstract public String usingKeyspace();
	
	abstract public boolean isShowCql();
	
	abstract public Executor getExecutor();
	
	abstract public ColumnValueProvider getValueProvider();
	
	abstract public ColumnValuePreparer getValuePreparer();

	public PreparedStatement prepare(RegularStatement statement) {
		
		try {
			
			log(statement);
			
			return currentSession().prepare(statement);
			
		}
		catch(RuntimeException e) {
			throw translateException(e);
		}
		
	}
	
	public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
		
		try {
			
			log(statement);
			
			return currentSession().prepareAsync(statement);
			
		}
		catch(RuntimeException e) {
			throw translateException(e);
		}
		
	}
	
	public ResultSet execute(Statement statement) {
		
		return executeAsync(statement).getUninterruptibly();
		
	}
	
	public ResultSetFuture executeAsync(Statement statement) {
		
		try {
			
			log(statement);
			
			return currentSession().executeAsync(statement);
			
		}
		catch(RuntimeException e) {
			throw translateException(e);
		}
		
	}

	void log(Statement statement) {
		if (logger.isInfoEnabled()) {
			logger.info("Execute statement " + statement);
		}
		
		if (isShowCql()) {
			
			if (statement instanceof BuiltStatement) {
				
				BuiltStatement builtStatement = (BuiltStatement) statement;

				RegularStatement regularStatement = builtStatement.setForceNoValues(true);
				
				System.out.println(regularStatement.getQueryString());
			}
			else if (statement instanceof RegularStatement) {
				
				RegularStatement regularStatement = (RegularStatement) statement;
				
				System.out.println(regularStatement.getQueryString());
				
			}
			else {
				System.out.println(statement.toString());
			}
			

		}
	}
	
	RuntimeException translateException(RuntimeException e) {
		
		if (e instanceof CasserException) {
			return e;
		}
		
		throw new CasserException(e);
	}
	
}
