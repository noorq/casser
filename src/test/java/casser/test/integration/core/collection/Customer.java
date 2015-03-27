package casser.test.integration.core.collection;

import java.util.List;
import java.util.Map;
import java.util.Set;

import casser.mapping.DataTypeName;
import casser.mapping.PartitionKey;
import casser.mapping.Table;

import com.datastax.driver.core.DataType.Name;

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
