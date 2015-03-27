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
package casser.mapping.convert;

import java.util.function.Function;

import casser.mapping.MapExportable;
import casser.mapping.UDTUtil;
import casser.support.CasserMappingException;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public final class EntityToUDTValueConverter implements Function<Object, UDTValue> {

	private final UserType userType;
	
	public EntityToUDTValueConverter(UserType userType) {
		this.userType = userType;
	}
	
	@Override
	public UDTValue apply(Object source) {
		
		UDTValue udtValue = userType.newValue();
		
		if (!(source instanceof MapExportable)) {
			throw new CasserMappingException("instance must be MapExportable " + source);
		}
		
		MapExportable exportable = (MapExportable) source;
		
		UDTUtil.write(udtValue, exportable.toMap());
		
		return udtValue;
	}

}
