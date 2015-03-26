/*
 *      Copyright (C) 2015 Noorq, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package casser.test.integration.core.usertype;

import casser.mapping.Field;
import casser.mapping.UserDefinedType;

@UserDefinedType("address0")
public interface Address {

	@Field("line_1")
	String getStreet();

	void setStreet(String street);

	String getCity();

	void setCity(String city);

	int getZip();

	void setZip(int zip);

	String getCountry();

	void setCountry(String country);

}
