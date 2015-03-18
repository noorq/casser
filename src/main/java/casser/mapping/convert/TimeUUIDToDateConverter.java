package casser.mapping.convert;

import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

public enum TimeUUIDToDateConverter implements Function<UUID, Date> {

	INSTANCE;

	@Override
	public Date apply(UUID source) {
		return new Date(TimeUUIDUtil.getTimestampMillis(source));
	}

}
