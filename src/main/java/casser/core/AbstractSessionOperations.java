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

import casser.mapping.CasserMappingRepository;
import casser.mapping.ColumnValuePreparer;
import casser.support.CasserException;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractSessionOperations {

	final Logger logger = LoggerFactory.getLogger(getClass());
	
	abstract public Session currentSession();
	
	abstract public String usingKeyspace();
	
	abstract public boolean isShowCql();
	
	abstract public Executor getExecutor();
	
	abstract public CasserMappingRepository getRepository();
	
	abstract public ColumnValuePreparer getValuePreparer();
	
	public ResultSet execute(Statement statement) {
		
		return executeAsync(statement).getUninterruptibly();
		
	}
	
	public ResultSetFuture executeAsync(Statement statement) {
		
		try {
			
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
			
			return currentSession().executeAsync(statement);
			
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
