package casser.mapping;

public enum Ordering {

	ASCENDING("ASC"),

	DESCENDING("DESC");

	private final String cql;

	private Ordering(String cql) {
		this.cql = cql;
	}

	public String cql() {
		return cql;
	}
	
}