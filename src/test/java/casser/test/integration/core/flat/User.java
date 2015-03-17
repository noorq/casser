package casser.test.integration.core.flat;

import casser.mapping.Table;

@Table("user")
public interface User {

	Long getId();
	
	void setId(Long id);
	
	String getName();
	
	void setName(String name);
	
	Integer getAge();
	
	void setAge(Integer age);
	
}
