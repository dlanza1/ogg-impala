package ch.cern.impala.ogg.datapump.oracle;

import java.io.IOException;

@SuppressWarnings("serial")
public class FileFormatException extends IOException {

	public FileFormatException(String message) {
		super(message);
	}

}
