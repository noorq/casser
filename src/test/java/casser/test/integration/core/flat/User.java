package casser.test.integration.core.flat;

import casser.mapping.Column;
import casser.mapping.KeyType;
import casser.mapping.PrimaryKey;
import casser.mapping.Table;

@Table("user")
public interface User {

	@PrimaryKey(type=KeyType.PARTITION, ordinal=1)
	Long getId();
	
	void setId(Long id);
	
	@Column("override_name")
	String getName();
	
	void setName(String name);
	
	Integer getAge();
	
	void setAge(Integer age);
	
}
