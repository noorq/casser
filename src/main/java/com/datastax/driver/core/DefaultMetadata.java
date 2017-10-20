package com.datastax.driver.core;

import java.util.Arrays;
import java.util.List;

public class DefaultMetadata extends Metadata {

	public DefaultMetadata() {
		super(null);
	}

	private DefaultMetadata(Cluster.Manager cluster) {
		super(cluster);
	}

	public TupleType newTupleType(DataType... types) {
		return newTupleType(Arrays.asList(types));
	}

	public TupleType newTupleType(List<DataType> types) {
		return new TupleType(types, ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
	}
}
