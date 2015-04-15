package com.noorq.casser.test.integration.core.usertype;

import com.noorq.casser.mapping.annotation.column.Column;
import com.noorq.casser.mapping.annotation.entity.UDT;

@UDT
public interface AddressInformation {

	@Column
	Address address();
	
}
