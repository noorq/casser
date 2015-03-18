package casser.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import casser.support.CasserException;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractSessionOperations {

	final Logger logger = LoggerFactory.getLogger(getClass());
	
	abstract Session currentSession();
	
	abstract boolean isShowCql();
	
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
	
	public ResultSet execute(BuiltStatement statement) {
		
		try {
			
			if (logger.isInfoEnabled()) {
				logger.info("Execute statement " + statement);
			}
			
			if (isShowCql()) {
				
				RegularStatement regular = statement.setForceNoValues(true);
				
				String cql = regular.getQueryString();
				
				System.out.println(cql);
				
				return currentSession().execute(regular);
			}
			else {

				return currentSession().execute(statement);

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
