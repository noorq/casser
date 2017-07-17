/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.mapping.validator;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public interface SizeConstraint {

	static final int[] EMPTY = new int[0];

	default int[] getSize(Object value) {

		if (value == null) {
			return null;
		}

		if (value.getClass().isArray()) {
			return new int[]{Array.getLength(value)};
		}

		if (value instanceof CharSequence) {
			CharSequence seq = (CharSequence) value;
			return new int[]{seq.length()};
		}

		if (value instanceof ByteBuffer) {
			ByteBuffer bb = (ByteBuffer) value;
			return new int[]{bb.position()};
		}

		if (value instanceof Collection) {
			Collection<?> col = (Collection<?>) value;
			return new int[]{col.size()};
		}

		if (value instanceof Map) {
			Map<?, ?> map = (Map<?, ?>) value;
			return new int[]{map.size()};
		}

		return EMPTY;
	}

}
