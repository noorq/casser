package casser.test.integration.core.compound;

import java.util.Date;
import java.util.UUID;

import casser.mapping.ClusteringColumn;
import casser.mapping.Column;
import casser.mapping.PartitionKey;
import casser.mapping.Qualify;

import com.datastax.driver.core.DataType.Name;

public interface Timeline {

	@PartitionKey
	UUID getUserId();
	
	void setUserId(UUID uid);
	
	@ClusteringColumn
	@Qualify(type=Name.TIMEUUID)
	Date getTimestamp();
	
	void setTimestamp(Date ts);
	
	@Column
	String getText();
	
	void setText(String text);
	
	
}
