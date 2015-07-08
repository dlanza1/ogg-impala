package ch.cern.impala.ogg.datapump.utils;

@SuppressWarnings("serial")
public class BadConfigurationException extends Exception {

	public BadConfigurationException(String error_message) {
		super(error_message);
	}

}
