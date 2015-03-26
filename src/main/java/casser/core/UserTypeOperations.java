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
package casser.core;

import casser.mapping.CasserMappingEntity;

import com.datastax.driver.core.UserType;

public final class UserTypeOperations {
	
	private final AbstractSessionOperations sessionOps;
	
	public UserTypeOperations(AbstractSessionOperations sessionOps) {
		this.sessionOps = sessionOps;
	}
	
	public void createUserType(String name, CasserMappingEntity<?> entity) {
		
		
		
	}

	public void validateUserType(String name, UserType userType, CasserMappingEntity<?> entity) {
		
	}

	
	public void updateUserType(String name, UserType userType, CasserMappingEntity<?> entity) {
		
	}


}
