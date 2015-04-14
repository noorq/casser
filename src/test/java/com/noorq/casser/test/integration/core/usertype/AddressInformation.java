package com.noorq.casser.test.integration.core.usertype;

import com.noorq.casser.mapping.annotation.entity.UserDefinedType;

@UserDefinedType
public interface AddressInformation {

	Address address();
	
}
