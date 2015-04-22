package ch.cern.impala.ogg.datapump;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
	private static final String EXT_CONTROL_FILE_TO_PROCESS = ".to_process";
	
	public static final CharSequence FILES_LOADED_INTO_HDFS_LABEL = "FILES LOADED INTO HDFS";

	public static final CharSequence DATA_INSERTED_INTO_FINAL_LABEL = "DATA INSERTED INTO FINAL TABLE";
	
	public ControlFile(String pathname) throws IOException {
		super(pathname);
	}

	public ControlFile getControlFileToProcess() throws IOException {
		ControlFile control_file_to_process = new ControlFile(
				getAbsolutePath().concat(EXT_CONTROL_FILE_TO_PROCESS));
		
		//If already exists, there was an error during previous process or it was stopped
		if(control_file_to_process.exists()){
			LOG.warn("recovering from previous control file to process");
			
			return control_file_to_process;
		}
		
		if(!exists())
			return null;
	
		//Rename this control file
		if(!renameTo(control_file_to_process)){
			LOG.error("the source control file " + this + " could not be renamed");
	
			throw new IOException("the source control file " + this + " could not be renamed");
		}
		
		return control_file_to_process;
	}

	public List<String> getDataFileNames() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(this));
		
		LinkedList<String> files = new LinkedList<String>();
		
        String line = br.readLine();
        while (line != null) {
        	if(line.equals(FILES_LOADED_INTO_HDFS_LABEL)){
        		LOG.warn("the files have been already loaded into HDFS");
        		break;
        	}
        	
            files.addAll(Arrays.asList(line.split(",")));
        	
            line = br.readLine();
        }
        
        try{
        	br.close();
        }catch(Exception e){}
        
        return files;
	}

	public void markAsFilesLoadedIntoHDFS() {
		try {
			FileWriter fw = new FileWriter(this);
			fw.append(FILES_LOADED_INTO_HDFS_LABEL);
			fw.close();
		} catch (IOException e) {
			LOG.error("the control file could not be mark as files loaded into HDFS", e);
		}
	}

	public void markAsDataInsertedIntoFinalTable() throws IOException {
		try {
			FileWriter fw = new FileWriter(this);
			fw.append(DATA_INSERTED_INTO_FINAL_LABEL);
			fw.close();
		} catch (IOException e) {
			LOG.error("FATAL: the control file could not be mark as data inserted into final table. "
					+ " IF YOU RUN THE LOADER, THE STAGING DATA WILL BE RE-INSERTED.", e);
			
			throw e;
		}
	}

	public boolean isMarkedAsFilesLoadedIntoHDFS() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(this));
		
		boolean out = br.readLine().equals(FILES_LOADED_INTO_HDFS_LABEL);
		
		try {
			br.close();
		} catch (IOException e) {}
		
		return out;
	}
	
	public boolean isMarkedAsDataInsertedIntoFinalTable() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(this));
		
		boolean out = br.readLine().equals(DATA_INSERTED_INTO_FINAL_LABEL);
		
		try {
			br.close();
		} catch (IOException e) {}
		
		return out;
	}
}
