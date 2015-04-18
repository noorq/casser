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
package com.noorq.casser.mapping.convert;

import java.util.Set;
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableSet;
import com.noorq.casser.core.SessionRepository;

public final class SetToUDTSetConverter extends AbstractUDTValueWriter implements Function<Object, Object> {

	public SetToUDTSetConverter(Class<?> iface, UserType userType, SessionRepository repository) {
		super(iface, userType, repository);
	}
	
	@Override
	public Object apply(Object t) {
		
		Set<Object> sourceSet = (Set<Object>) t;
		
		ImmutableSet.Builder<UDTValue> builder = ImmutableSet.builder();

		for (Object source : sourceSet) {
		
			UDTValue outValue = null;
			
			if (source != null) {
				outValue = userType.newValue();
				write(outValue, source);
			}
			
			builder.add(outValue);
				
	   }
		
		return builder.build();
	}


}
