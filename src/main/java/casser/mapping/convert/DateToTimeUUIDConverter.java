package casser.mapping.convert;

import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

/**
 * Simple Date to TimeUUID Converter
 * 
 */

public enum DateToTimeUUIDConverter implements Function<Date, UUID> {

	INSTANCE;

	@Override
	public UUID apply(Date source) {
		long milliseconds = source.getTime();
		return TimeUUIDUtil.createTimeUUID(milliseconds);
	}

}
