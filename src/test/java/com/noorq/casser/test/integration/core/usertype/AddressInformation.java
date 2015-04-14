package com.noorq.casser.test.integration.core.usertype;

import com.noorq.casser.mapping.annotation.column.Column;
import com.noorq.casser.mapping.annotation.entity.UserDefinedType;

@UserDefinedType
public interface AddressInformation {

	@Column(0)
	Address address();
	
}
