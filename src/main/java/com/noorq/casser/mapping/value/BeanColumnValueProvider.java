/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.mapping.value;

import java.lang.reflect.Method;

import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.support.CasserMappingException;

public enum BeanColumnValueProvider implements ColumnValueProvider {

	INSTANCE;
	
	@Override
	public <V> V getColumnValue(Object bean, int columnIndexUnused,
			CasserProperty property) {

		Method getter = property.getGetterMethod();
		
		Object value = null;
		try {
			value = getter.invoke(bean, new Object[] {});
		} catch (ReflectiveOperationException e) {
			throw new CasserMappingException("fail to call getter " + getter, e);
		} catch (IllegalArgumentException e) {
			throw new CasserMappingException("invalid getter " + getter, e);
		}
		
		return (V) value;
		
	}

}
