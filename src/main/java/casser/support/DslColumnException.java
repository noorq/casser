package casser.support;

import casser.core.ColumnInformation;

public class DslColumnException extends CasserException {

	private static final long serialVersionUID = -2745598205929757758L;

	private final ColumnInformation info;
	
	public DslColumnException(ColumnInformation info) {
		super("DSL Column Information Exception");
		this.info = info;
	}

	public ColumnInformation getColumnInformation() {
		return info;
	}
	
	
	
}
