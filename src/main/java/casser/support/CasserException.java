package casser.support;

public class CasserException extends RuntimeException {

	private static final long serialVersionUID = 7711799134283942588L;

	public CasserException(String msg) {
		super(msg);
	}

	public CasserException(Throwable t) {
		super(t);
	}

	public CasserException(String msg, Throwable t) {
		super(msg, t);
	}
	
}
