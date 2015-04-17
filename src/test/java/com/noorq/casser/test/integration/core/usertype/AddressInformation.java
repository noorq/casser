package com.noorq.casser.test.integration.core.usertype;

import com.noorq.casser.mapping.annotation.Column;
import com.noorq.casser.mapping.annotation.UDT;

@UDT
public interface AddressInformation {

	@Column
	Address address();
	
}
