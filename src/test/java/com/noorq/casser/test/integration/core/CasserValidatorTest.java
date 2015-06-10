package com.noorq.casser.test.integration.core;

import org.junit.Before;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserValidator;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.annotation.Constraints;
import com.noorq.casser.mapping.annotation.PartitionKey;
import com.noorq.casser.mapping.annotation.Table;
import com.noorq.casser.support.CasserException;
import com.noorq.casser.support.CasserMappingException;

public class CasserValidatorTest {

	@Table
	interface ModelForValidation {
		
		@Constraints.Email
		@PartitionKey
		String id();
		
	}
	
	CasserEntity entity;
	
	CasserProperty prop;
	
	@Before
	public void begin() {
		
		entity = Casser.entity(ModelForValidation.class);
		
		prop = entity.getProperty("id");
		
	}
	
	@Test(expected=CasserMappingException.class)
	public void testWrongType() {
		CasserValidator.INSTANCE.validate(prop, Integer.valueOf(123));
	}

	@Test(expected=CasserException.class)
	public void testWrongValue() {
		CasserValidator.INSTANCE.validate(prop, "123");
	}


	public void testOk() {
		CasserValidator.INSTANCE.validate(prop, "a@b.c");
	}
}
