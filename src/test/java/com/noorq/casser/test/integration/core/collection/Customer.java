package com.noorq.casser.test.integration.core.collection;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.DataType.Name;
import com.noorq.casser.mapping.DataTypeName;
import com.noorq.casser.mapping.PartitionKey;
import com.noorq.casser.mapping.Table;

@Table
public interface Customer {

	@PartitionKey
	UUID id();
	
	@DataTypeName(value = Name.SET, types={Name.TEXT})
	Set<String> aliases();
	
	@DataTypeName(value = Name.LIST, types={Name.TEXT})
	List<String> name();
	
	@DataTypeName(value = Name.MAP, types={Name.TEXT, Name.TEXT})
	Map<String, String> properties();

}
