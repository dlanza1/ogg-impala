package ch.cern.impala.ogg.datapump;

import java.io.File;
import java.io.IOException;

public class FileException extends IOException {

	private static final long serialVersionUID = 1L;
	
	File file_;
	
	public FileException(File file) {
		file_ = file;
	}

}
