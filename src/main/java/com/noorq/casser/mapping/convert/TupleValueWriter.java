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
package com.noorq.casser.mapping.convert;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.value.TupleColumnValuePreparer;

public class TupleValueWriter extends AbstractEntityValueWriter<TupleValue> implements Function<Object, TupleValue> {

	private final TupleType tupleType;
	private final TupleColumnValuePreparer valuePreparer;
	
	public TupleValueWriter(Class<?> iface, TupleType tupleType, SessionRepository repository) {
		super(iface);
		
		this.tupleType = tupleType;
		this.valuePreparer = new TupleColumnValuePreparer(tupleType, repository);
	}

	@Override
	void writeColumn(TupleValue udtValue, Object value,
			CasserProperty prop) {
		
		ByteBuffer bytes = (ByteBuffer) valuePreparer.prepareColumnValue(value, prop);
		
		if (bytes != null) {
			udtValue.setBytesUnsafe(prop.getOrdinal(), bytes);
		}
	}
	
	@Override
	public TupleValue apply(Object source) {
		if (source != null) {
			TupleValue outValue = tupleType.newValue();
			write(outValue, source);
			return outValue;
		}
		return null;
	}
	
}
