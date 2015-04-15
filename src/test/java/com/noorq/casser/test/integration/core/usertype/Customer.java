package com.noorq.casser.test.integration.core.usertype;

import java.util.UUID;

import com.noorq.casser.mapping.annotation.column.Column;
import com.noorq.casser.mapping.annotation.column.PartitionKey;
import com.noorq.casser.mapping.annotation.entity.Table;

@Table
public interface Customer {

	@PartitionKey
	UUID id();
	
	@Column
	AddressInformation addressInformation();
	
}
