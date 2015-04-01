package com.noorq.casser.test.integration.core.index;

import com.noorq.casser.mapping.Column;
import com.noorq.casser.mapping.Index;
import com.noorq.casser.mapping.PartitionKey;
import com.noorq.casser.mapping.Table;

@Table("books")
public interface Book {

	@PartitionKey
	long id();
	
	@Column
	@Index
	String isbn();
	
	@Column
	String author();
	
}
