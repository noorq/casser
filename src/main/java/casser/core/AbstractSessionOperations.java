package casser.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import casser.support.CasserException;

import com.datastax.driver.core.Session;

public abstract class AbstractSessionOperations {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	abstract Session currentSession();
	
	void doExecute(String cql) {
		
		try {
			
			logger.info("Execute query " + cql);
			
			currentSession().execute(cql);
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
