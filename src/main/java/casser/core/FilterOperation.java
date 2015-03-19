package casser.core;

import java.util.HashMap;
import java.util.Map;

public enum FilterOperation {

	EQUAL("=="),
	
	IN("in"),

	GREATER(">"),

	LESSER("<"),

	GREATER_OR_EQUAL(">="),

	LESSER_OR_EQUAL("<=");
	
	private final String operator;
	
	private final static Map<String, FilterOperation> indexByOperator = new HashMap<String, FilterOperation>();
	
	static {
		for (FilterOperation fo : FilterOperation.values()) {
			indexByOperator.put(fo.getOperator(), fo);
		}
	}
	
	private FilterOperation(String operator) {
		this.operator = operator;
	}

	public String getOperator() {
		return operator;
	}
	
	public static FilterOperation findByOperator(String operator) {
		return indexByOperator.get(operator);
	}
	
}
