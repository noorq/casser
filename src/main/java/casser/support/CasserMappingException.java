package casser.support;

public class CasserMappingException extends CasserException {

	private static final long serialVersionUID = -4730562130753392363L;

	public CasserMappingException(String msg) {
		super(msg);
	}

	public CasserMappingException(Throwable t) {
		super(t);
	}

	public CasserMappingException(String msg, Throwable t) {
		super(msg, t);
	}
	
}
