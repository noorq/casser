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
package com.noorq.casser.support;

import com.noorq.casser.core.reflect.CasserPropertyNode;

public final class DslPropertyException extends CasserException {

	private static final long serialVersionUID = -2745598205929757758L;

	private final CasserPropertyNode propertyNode;
	
	public DslPropertyException(CasserPropertyNode propertyNode) {
		super("DSL PropertyNode Exception");
		this.propertyNode = propertyNode;
	}

	public CasserPropertyNode getPropertyNode() {
		return propertyNode;
	}

}
