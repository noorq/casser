package com.noorq.casser.test.integration.core.prepared;

import com.noorq.casser.mapping.Column;
import com.noorq.casser.mapping.PartitionKey;
import com.noorq.casser.mapping.Table;

@Table("cars")
public interface Car {

	@PartitionKey(ordinal=1)
	String make();
	
	@PartitionKey(ordinal=2)
	String model();

	@Column
	int year();
	
	@Column
	double price();
	
}
