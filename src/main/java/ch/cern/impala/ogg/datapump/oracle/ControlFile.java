package ch.cern.impala.ogg.datapump.oracle;

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

import com.google.common.base.Preconditions;

public class ControlFile extends File{
	private static final long serialVersionUID = 1L;
	
	final private static Logger LOG = LoggerFactory.getLogger(ControlFile.class);

	/**
	 * Extension of the control file to process
	 */
	private static final String EXT_CONTROL_FILE_TO_PROCESS = ".to_process";
	
	public static final CharSequence FILES_LOADED_INTO_HDFS_LABEL = "FILES LOADED INTO HDFS";

	public static final CharSequence DATA_INSERTED_INTO_FINAL_TABLE_LABEL = "DATA INSERTED INTO FINAL TABLE";
	
	private enum State {NOT_INITIALIZED, TO_PROCESS, LOADED_INTO_HDFS, INSERTED_INTO_FINAL_TABLE};
	
	private State state = State.NOT_INITIALIZED;
	
	public ControlFile(String pathname) throws IOException {
		super(pathname);
	}

	private ControlFile setState() throws IOException {
		Preconditions.checkArgument(state == State.NOT_INITIALIZED);
		
		BufferedReader br = new BufferedReader(new FileReader(this));
		
		String line = br.readLine();
		
		if(line.equals(FILES_LOADED_INTO_HDFS_LABEL))
			state = State.LOADED_INTO_HDFS;
		else if(line.equals(DATA_INSERTED_INTO_FINAL_TABLE_LABEL))
			state = State.INSERTED_INTO_FINAL_TABLE;
		else
			state = State.TO_PROCESS;
		
		try {
			br.close();
		} catch (IOException e) {}
		
		return this;
	}

	public ControlFile getControlFileToProcess() throws IOException {
		Preconditions.checkArgument(state == State.NOT_INITIALIZED);
		
		ControlFile control_file_to_process = new ControlFile(
				getAbsolutePath().concat(EXT_CONTROL_FILE_TO_PROCESS));
		
		//If already exists, there was an error during previous process or it was stopped
		if(control_file_to_process.exists()){
			LOG.warn("recovering from previous control file to process");
			
			return control_file_to_process.setState();
		}
		
		if(!exists())
			return null;
	
		//Rename this control file
		if(!renameTo(control_file_to_process)){
			LOG.error("the source control file " + this + " could not be renamed");
	
			throw new IOException("the source control file " + this + " could not be renamed");
		}else{
			LOG.debug("the control file " + this + " has been renamed to " + control_file_to_process);
		}
		
		return control_file_to_process.setState();
	}

	public List<String> getDataFileNames() throws IOException {
		Preconditions.checkArgument(state == State.TO_PROCESS);
		
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

	public void filesLoadedIntoHDFS() throws IOException {
		Preconditions.checkArgument(state == State.TO_PROCESS);
		
		try {
			FileWriter fw = new FileWriter(this);
			fw.append(FILES_LOADED_INTO_HDFS_LABEL);
			fw.close();
		} catch (IOException e) {
			LOG.error("the control file could not be mark as files loaded into HDFS", e);
			
			throw e;
		}
		
		state = State.LOADED_INTO_HDFS;
	}

	public void markAsDataInsertedIntoFinalTable() throws IOException {
		Preconditions.checkArgument(state == State.LOADED_INTO_HDFS);
		
		try {
			FileWriter fw = new FileWriter(this);
			fw.append(DATA_INSERTED_INTO_FINAL_TABLE_LABEL);
			fw.close();
		} catch (IOException e) {
			LOG.error("FATAL: the control file could not be mark as data inserted into final table. "
					+ " IF YOU RUN THE LOADER, THE STAGING DATA WILL BE RE-INSERTED.", e);
			
			throw e;
		}
		
		state = State.INSERTED_INTO_FINAL_TABLE;
	}

	public boolean canDataBeLoadedIntoHDFS() throws IOException {
		return state == State.TO_PROCESS;
	}
	
	public boolean canDataBeInsertedIntoFinalTable() throws IOException {
		return state == State.LOADED_INTO_HDFS;
	}

}
