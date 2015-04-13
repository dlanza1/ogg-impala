package ch.cern.impala.ogg.datapump;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Batch {
	
	final private static Logger LOG = LoggerFactory.getLogger(Batch.class);

	private String sourceLocalDirectory;
	private Path stagingHDFSDirectory;

	private ControlFile controlFile;
	
	private ITable targetTable; 

	public Batch(ControlFile controlFile, ITable targetTable, PropertiesE prop) 
			throws IOException, ClassNotFoundException, SQLException {
		
		this.controlFile = controlFile;
		this.stagingHDFSDirectory = prop.getStagingHDFSDirectory();
		this.sourceLocalDirectory = prop.getSourceLocalDirectory();
		
		this.targetTable = targetTable;
	}

	public void start() throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		FileSystem local = FileSystem.getLocal(new Configuration());
		
		//Create empty HDFS directory
		if(hdfs.exists(stagingHDFSDirectory)){
			if(!hdfs.delete(stagingHDFSDirectory, true)){
				IllegalStateException e = new IllegalStateException("target directory could not be deleted");
				LOG.error(e.getMessage(), e);
				throw e;
			}
		}
		if(!hdfs.mkdirs(stagingHDFSDirectory)){
			IllegalStateException e = new IllegalStateException("target directory could not be created");
			LOG.error(e.getMessage(), e);
			throw e;
		}
		
		//Move files to HDFS
		List<String> files = this.controlFile.getDataFileNames();
		for (String file : files) {
			Path path = new Path(sourceLocalDirectory + "/" + file);
			
			try{
				hdfs.copyFromLocalFile(path, stagingHDFSDirectory);
				
				LOG.debug("the local file " + path + " has been moved to " + stagingHDFSDirectory + " (HDFS)");
			}catch(Exception e){
				LOG.error("the local file " + path + " could not be copied to " + stagingHDFSDirectory + " (HDFS)");
				
				throw e;
			}
		}
		
		//Delete all copied files
		for (String file : files) {
			Path path = new Path(sourceLocalDirectory + "/" + file);
			
			if(local.delete(path, true)){
				LOG.debug(file + " has been deleted");
			}else{
				throw new IllegalStateException("REQUIRED MANUAL FIX: the file " + file 
						+ " could not be deleted (SOLUTION: the files contained in " + controlFile 
						+ " must be deleted and this control file should contain: \"" 
						+ ControlFile.FILES_LOADED_INTO_HDFS_LABEL + "\")");
			}
		}
		
		//Write label in control file to avoid re-load
		controlFile.markAsFilesLoadedIntoHDFS();
		
		LOG.info(files.size() + " files have been moved to " + stagingHDFSDirectory + " (HDFS)");
		
		if(controlFile.isMarkedAsFilesLoadedIntoHDFS()){
			//Create Impala staging table
			Path tableDir = hdfs.resolvePath(stagingHDFSDirectory);
			ITable externalTable = targetTable.createStagingTable(tableDir); 
			
			//Insert staging data into target table
			externalTable.insertoInto(targetTable);
			
			//Write label in control file to avoid re-insert
			controlFile.markAsDataInsertedIntoFinalTable();
			
			//Remove staging data
			try{
				externalTable.drop();
			}catch(SQLException e){
				LOG.error("the Impala table " + externalTable + " which contains "
						+ "the staging data could not be deleted", e);
			}
			try{
				hdfs.delete(tableDir, true);
			}catch(Exception e){
				LOG.error("the HDFS directory " + tableDir + " which contains "
						+ "the data of the staging table could not be deleted", e);
			}
		}
		
		try{
			//Delete control file
			controlFile.delete();
		}catch(Exception e){
			LOG.error("the control file " + controlFile + " could not be deleted", e);
		}
	}

}
