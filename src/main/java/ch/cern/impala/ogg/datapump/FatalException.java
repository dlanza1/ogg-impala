package ch.cern.impala.ogg.datapump;

@SuppressWarnings("serial")
public class FatalException extends Exception {

	public FatalException(Exception e) {
		super(e);
	}

}
