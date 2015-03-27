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
package casser.core.reflect;

import java.lang.reflect.Proxy;

import casser.core.Instantiator;

public enum ReflectionDslInstantiator implements Instantiator {

	INSTANCE;
	
	@Override
	@SuppressWarnings("unchecked")
	public <E> E instantiate(Class<E> iface, ClassLoader classLoader) {
		DslInvocationHandler<E> handler = new DslInvocationHandler<E>(iface);
		E proxy = (E) Proxy.newProxyInstance(
		                            classLoader,
		                            new Class[] { iface },
		                            handler);
		return proxy;
	}



}
