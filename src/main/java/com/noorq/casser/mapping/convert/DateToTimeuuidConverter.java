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

import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

import com.noorq.casser.support.Timeuuid;

/**
 * Simple Date to TimeUUID Converter
 * 
 */

public enum DateToTimeuuidConverter implements Function<Date, UUID> {

	INSTANCE;

	@Override
	public UUID apply(Date source) {
		long milliseconds = source.getTime();
		return Timeuuid.of(milliseconds);
	}

}
