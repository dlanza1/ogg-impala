package ch.cern.impala.ogg.datapump;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.impala.Query;
import ch.cern.impala.ogg.datapump.oracle.ControlFile;

public class Batch {
	
	final private static Logger LOG = LoggerFactory.getLogger(Batch.class);

	//File systems
	private FileSystem local;
	private FileSystem hdfs;
	
	/**
	 * Source data control file
	 */
	private ControlFile controlFile;
	
	//Staging data
	private Path stagingHDFSDirectory;
	
	private Query dropStagingTable;
	private Query createStagingTable;
	private Query insertInto;

	private List<String> dataFiles;

	public Batch(FileSystem local, 
			FileSystem hdfs, 
			ControlFile controlFile, 
			Path stagingHDFSDirectory,
			Query dropStagingTable,
			Query createStagingTable,
			Query insertInto) 
			throws IOException, ClassNotFoundException, SQLException {
		this.local = local;
		this.hdfs = hdfs;
		this.controlFile = controlFile;
		this.stagingHDFSDirectory = stagingHDFSDirectory;
		this.dropStagingTable = dropStagingTable;
		this.createStagingTable = createStagingTable;
		this.insertInto = insertInto;
		
		// Get data file names
		dataFiles = controlFile.getDataFileNames();
	}

	public void start() throws Exception {
			
		// Create staging directory
		if(!hdfs.mkdirs(stagingHDFSDirectory)){
			IllegalStateException e = new IllegalStateException(
							"staging directory could not be created");
			LOG.error(e.getMessage(), e);
			throw e;
		}
		
		// Copy data files to HDFS
		copyDataFilesToHDFS(local, hdfs);
		
		// Create staging table
		createStagingTable.exect();
		LOG.info("created staging table");
			
		// Insert staging table data into final table
		insertInto.exect();
		LOG.info("copied data from staging table to final table");
	}

	private void copyDataFilesToHDFS(FileSystem local, FileSystem hdfs) throws Exception {
		
		//Copy all files into HDFS
		long totalSize = 0;
		for (String file : dataFiles) {
			Path path = new Path(file);
			
			long length = 0;
			try{
				length = local.getFileStatus(path).getLen();
			}catch(IOException e){}
			
			try{
				hdfs.copyFromLocalFile(path, stagingHDFSDirectory);

				totalSize += length;
				
				LOG.debug("the local file " + path + " (" + length 
						+ " bytes) has been copied to " + stagingHDFSDirectory);
			}catch(Exception e){
				LOG.error("the local file " + path + " could not be copied to HDFS", e);
				throw e;
			}
		}
		
		LOG.info(dataFiles.size() + " files " + "("+ totalSize + " bytes) have been copied to HDFS");
	}

	/**
	 * Remove control file and staging data
	 * 
	 * @throws Exception
	 */
	public void clean() throws Exception {

		//Delete control file
		try{
			controlFile.delete();
		}catch(Exception e){
			LOG.error("the control file " + controlFile + " could not be deleted", e);
			LOG.error("the control file \"" + controlFile + "\" must be deleted before"
					+ " starting again the loader, otherwise data will be "
					+ " reinserted into final table (duplicates)");
			
			throw e;
		}
		
		//Delete all local data files
		for (String file : dataFiles) {
			Path path = new Path(file);
			
			if(local.delete(path, true)){
				LOG.debug(file + " has been deleted");
			}else{
				throw new IllegalStateException("the data file " + file + " could not be deleted");
			}
		}
		
		//Remove Impala staging table
		try{
			dropStagingTable.exect();
			
			LOG.debug("Impala staging table has been dropped");
		}catch(SQLException e){
			LOG.error("the staging table could not be deleted", e);
			
			throw e;
		}
		
		//Remove staging data stored in HDFS
		try{
			hdfs.delete(stagingHDFSDirectory, true);
			
			LOG.debug("Staging directory in HDFS (" + stagingHDFSDirectory + ") has been removed");
		}catch(Exception e){
			LOG.error("the HDFS directory " + stagingHDFSDirectory + " which contains "
					+ "the data of the staging table could not be deleted", e);
			throw e;
		}
		
		LOG.info("deleted staging data");
	}

}
