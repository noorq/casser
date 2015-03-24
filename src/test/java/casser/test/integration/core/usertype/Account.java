package casser.test.integration.core.usertype;

import casser.mapping.PartitionKey;
import casser.mapping.Table;

@Table
public interface Account {

	@PartitionKey
	long getId();
	
	void setId(long id);
	
	Address getAddress();
	
	void setAddress(Address address);
	
}
