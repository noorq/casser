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
package net.helenus.mapping.convert;

import java.util.Map;

import net.helenus.core.Helenus;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.value.BeanColumnValueProvider;

public abstract class AbstractEntityValueWriter<V> {

	final HelenusEntity entity;

	public AbstractEntityValueWriter(Class<?> iface) {
		this.entity = Helenus.entity(iface);
	}

	abstract void writeColumn(V outValue, Object value, HelenusProperty prop);

	public void write(V outValue, Object source) {

		if (source instanceof MapExportable) {

			MapExportable exportable = (MapExportable) source;

			Map<String, Object> propertyToValueMap = exportable.toMap();

			for (Map.Entry<String, Object> entry : propertyToValueMap.entrySet()) {

				Object value = entry.getValue();

				if (value == null) {
					continue;
				}

				HelenusProperty prop = entity.getProperty(entry.getKey());

				if (prop != null) {

					writeColumn(outValue, value, prop);
				}
			}

		} else {

			for (HelenusProperty prop : entity.getOrderedProperties()) {

				Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(source, -1, prop);

				if (value != null) {
					writeColumn(outValue, value, prop);
				}
			}
		}
	}
}
