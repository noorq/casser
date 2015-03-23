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
package casser.core;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import casser.support.CasserException;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractSessionOperations {

	final Logger logger = LoggerFactory.getLogger(getClass());
	
	abstract public Session currentSession();
	
	abstract public boolean isShowCql();
	
	abstract public Executor getExecutor();
	
	public ResultSet execute(String cql) {
		
		try {
			
			if (logger.isInfoEnabled()) {
				logger.info("Execute query " + cql);
			}
			
			if (isShowCql() && cql != null) {
				System.out.println(cql);
			}
			
			return currentSession().execute(cql);
		}
		catch(RuntimeException e) {
			throw translateException(e);
		}
		
	}
	
	public ResultSetFuture executeAsync(BuiltStatement statement) {
		
		try {
			
			if (logger.isInfoEnabled()) {
				logger.info("Execute statement " + statement);
			}
			
			if (isShowCql()) {
				
				RegularStatement regular = statement.setForceNoValues(true);
				
				String cql = regular.getQueryString();
				
				System.out.println(cql);
				
				return currentSession().executeAsync(regular);
			}
			else {

				return currentSession().executeAsync(statement);

			}
			
		}
		catch(RuntimeException e) {
			throw translateException(e);
		}
		
	}
	
	RuntimeException translateException(RuntimeException e) {
		
		if (e instanceof CasserException) {
			return e;
		}
		
		throw new CasserException(e);
	}
	
}
