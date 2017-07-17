package net.helenus.test.integration.core.usertype;

import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.UDT;

@UDT
public interface AddressInformation {

	@Column
	Address address();

}
