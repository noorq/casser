package casser.core.operation;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

import casser.core.AbstractSessionOperations;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.MappingUtil;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class UpsertOperation extends AbstractObjectOperation<ResultSet, UpsertOperation> {

	private final Insert insert;
	
	public UpsertOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity<?> entity, Object pojo) {
		super(sessionOperations);
		
		this.insert = QueryBuilder.insertInto(entity.getTableName());
		
		for (CasserMappingProperty<?> prop : entity.getMappingProperties()) {
			
			Method getter = prop.getGetterMethod();
			
			Object value = null;
			try {
				value = getter.invoke(pojo, new Object[] {});
			} catch (ReflectiveOperationException e) {
				throw new CasserMappingException("fail to call getter " + getter, e);
			} catch (IllegalArgumentException e) {
				throw new CasserMappingException("invalid getter " + getter, e);
			}
			
			value = MappingUtil.prepareValueForWrite(prop, value);
			
			if (value != null) {
				insert.value(prop.getColumnName(), value);
			}
			
		}
		
		
	}

	@Override
	public BuiltStatement buildStatement() {
		return insert;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
	
	
}
