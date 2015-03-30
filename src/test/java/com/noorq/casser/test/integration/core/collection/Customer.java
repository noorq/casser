package com.noorq.casser.test.integration.core.collection;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.DataType.Name;
import com.noorq.casser.mapping.DataTypeName;
import com.noorq.casser.mapping.PartitionKey;
import com.noorq.casser.mapping.Table;

@Table
public interface Customer {

	@PartitionKey
	int id();
	
	@DataTypeName(value = Name.SET, typeParameters={Name.TEXT})
	Set<String> aliases();
	
	@DataTypeName(value = Name.LIST, typeParameters={Name.TEXT})
	List<String> name();
	
	@DataTypeName(value = Name.MAP, typeParameters={Name.TEXT, Name.TEXT})
	Map<String, String> properties();

}
