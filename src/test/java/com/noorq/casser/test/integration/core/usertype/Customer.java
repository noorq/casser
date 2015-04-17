package com.noorq.casser.test.integration.core.usertype;

import java.util.UUID;

import com.noorq.casser.mapping.annotation.Column;
import com.noorq.casser.mapping.annotation.PartitionKey;
import com.noorq.casser.mapping.annotation.Table;

@Table
public interface Customer {

	@PartitionKey
	UUID id();
	
	@Column
	AddressInformation addressInformation();
	
}
