package casser.support;

import casser.mapping.CasserProperty;

public class DslPropertyException extends CasserException {

	private static final long serialVersionUID = -2745598205929757758L;

	private final CasserProperty property;
	
	public DslPropertyException(CasserProperty property) {
		super("DSL Property Exception");
		this.property = property;
	}

	public CasserProperty getProperty() {
		return property;
	}
	
	
	
}
