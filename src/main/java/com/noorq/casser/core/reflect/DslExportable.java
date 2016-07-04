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
package com.noorq.casser.core.reflect;

import com.noorq.casser.mapping.CasserEntity;


public interface DslExportable {
	
	public static final String GET_ENTITY_METHOD = "getCasserMappingEntity";
	public static final String GET_PARENT_METHOD = "getParentDslCasserPropertyNode";
	
	CasserEntity getCasserMappingEntity();
	
	CasserPropertyNode getParentDslCasserPropertyNode();
	
}
