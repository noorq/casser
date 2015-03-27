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
package casser.mapping;

import java.util.Optional;
import java.util.function.Function;

import casser.mapping.convert.ConverterRepositoryAware;


public final class StatementColumnValuePreparer implements ColumnValuePreparer {

	private final CasserMappingRepository repository;

	public StatementColumnValuePreparer(CasserMappingRepository repository) {
		this.repository = repository;
	}
	
	@Override
	public Object prepareColumnValue(Object value, CasserMappingProperty prop) {

		if (value != null) {

			Optional<Function<Object, Object>> converter = prop
					.getWriteConverter();

			if (converter.isPresent()) {
				
				Function<Object, Object> fn = converter.get();
				
				if (fn instanceof ConverterRepositoryAware) {
					((ConverterRepositoryAware) fn).setRepository(repository);
				}
				
				value = fn.apply(value);
			}

		}

		return value;
	}
	
}
