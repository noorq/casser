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

import casser.support.CasserMappingException;

public class TypedConverter<I, O> implements Function<Object, Object> {

	private final Class<I> inputType;
	private final Class<O> outputType;
	private final Function<I, O> delegate;
	
	public TypedConverter(Class<I> inputType, Class<O> outputType, Function<I, O> delegate) {
		this.inputType = inputType;
		this.outputType = outputType;
		this.delegate = delegate;
	}
	
	public static <I, O> TypedConverter<I, O> create(Class<I> inputType, Class<O> outputType, Function<I, O> delegate) {
		return new TypedConverter<I, O>(inputType, outputType, delegate);
	}
	
	@Override
	public Object apply(Object inputUnknown) {
		
		if (inputUnknown == null) {
			return null;
		}
		
		if (!inputType.isAssignableFrom(inputUnknown.getClass())) {
			throw new CasserMappingException("expected " + inputType + " type for input parameter " + inputUnknown.getClass());
		}
		
		I input = (I) inputUnknown;
		
		O outputUnknown = delegate.apply(input);
		
		if (outputUnknown == null) {
			return null;
		}
		
		if (!outputType.isAssignableFrom(outputUnknown.getClass())) {
			throw new CasserMappingException("expected " + outputType + " type for output result " + outputUnknown.getClass());
		}
		
		return outputUnknown;
	}

}
