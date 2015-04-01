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
package com.noorq.casser.core;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.noorq.casser.mapping.OrderingDirection;

/**
 *  Sugar methods for the queries
 *
 */

public final class Query {

	private Query() {
	}

	public static BindMarker marker() {
		return QueryBuilder.bindMarker(); 
	}

	public static BindMarker marker(String name) {
		return QueryBuilder.bindMarker(name); 
	}

	public static Ordered asc(Getter<?> getter) {
		return new Ordered(getter, OrderingDirection.ASC);
	}

	public static Ordered desc(Getter<?> getter) {
		return new Ordered(getter, OrderingDirection.DESC);
	}

	public static <V> Postulate<V> eq(V val) {
		return new Postulate<V>(Operator.EQ, val);
	}
	
	public static <V> Postulate<V> lt(V val) {
		return new Postulate<V>(Operator.LT, val);
	}
	
	public static <V> Postulate<V> lte(V val) {
		return new Postulate<V>(Operator.LTE, val);
	}
	
	public static <V> Postulate<V> gt(V val) {
		return new Postulate<V>(Operator.GT, val);
	}
	
	public static <V> Postulate<V> gte(V val) {
		return new Postulate<V>(Operator.GTE, val);
	}

	public static <V> Postulate<V> in(Getter<V> getter, V[] vals) {
		return new Postulate<V>(Operator.IN, vals);
	}
	
    public static <K,V> Getter<V> get(Getter<List<V>> listGetter, int index) {
    	return null;
    }
	
    public static <K,V> Getter<V> get(Getter<Map<K, V>> mapGetter, K k) {
    	return null;
    }
    
    

}
