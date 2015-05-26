package ch.cern.impala.ogg.datapump.oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlFile extends File{
	private static final long serialVersionUID = 1L;
	
	final private static Logger LOG = LoggerFactory.getLogger(ControlFile.class);

	/**
	 * Extension of the control file to process
	 */
	private static final String EXT_CONTROL_FILE_TO_PROCESS = ".processing";
	
	public ControlFile(String pathname) throws IOException {
		super(pathname);
	}

	/**
	 * Get a copy (with a new name extension) of this control file
	 * 
	 * @return Copy of this control file
	 * 			- If this file does not exist, return null
	 * 			- If the copy already exists, return old copy
	 * @throws IOException
	 */
	public ControlFile getControlFileToProcess() throws IOException {
		
		ControlFile controlFileToProcess = new ControlFile(
				getAbsolutePath().concat(EXT_CONTROL_FILE_TO_PROCESS));
		
		//If already exists, there was an error during previous process or it was stopped
		//then recover from the old one
		if(controlFileToProcess.exists()){
			LOG.warn("recovering from previous control file to process");
			
			return controlFileToProcess;
		}
		
		//If tthe control file does not exist, can not be created a control file to process
		if(!exists())
			return null;
	
		//Rename this control file
		if(!renameTo(controlFileToProcess)){
			IOException e = new IOException("the source control file " + this + " could not be renamed");
			LOG.error(e.getMessage(), e);
			throw e;
		}else{
			LOG.debug("the control file " + this + " has been renamed to " + controlFileToProcess);
		}
		
		return controlFileToProcess;
	}

	/**
	 * Get the list of data file names
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getDataFileNames() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(this));
		
		LinkedList<String> files = new LinkedList<String>();
		
        String line = br.readLine();
        while (line != null) {
            files.addAll(Arrays.asList(line.split(",")));
        	
            line = br.readLine();
        }
        
        try{
        	br.close();
        }catch(Exception e){}
        
        return files;
	}

}
