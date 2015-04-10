package com.noorq.casser.test.integration.core.usertype;

import java.util.UUID;

import com.noorq.casser.mapping.PartitionKey;
import com.noorq.casser.mapping.Table;

@Table
public interface Customer {

	@PartitionKey
	UUID id();
	
	AddressInformation addressInformation();
	
}
