package ch.cern.impala.ogg.datapump;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;

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
	 * Source control files
	 */
	private LinkedList<ControlFile> controlFiles;
	
	//Staging data
	private Path stagingHDFSDirectory;
	
	private Query dropStagingTable;
	private Query createStagingTable;
	private Query insertInto;

	private LinkedList<String> dataFiles;

	public Batch(FileSystem local, 
			FileSystem hdfs, 
			LinkedList<ControlFile> controlFiles, 
			Path stagingHDFSDirectory,
			Query dropStagingTable,
			Query createStagingTable,
			Query insertInto) 
			throws IOException, SQLException {
		this.local = local;
		this.hdfs = hdfs;
		this.controlFiles = controlFiles;
		this.stagingHDFSDirectory = stagingHDFSDirectory;
		this.dropStagingTable = dropStagingTable;
		this.createStagingTable = createStagingTable;
		this.insertInto = insertInto;
		
		// Get data file names
		dataFiles = new LinkedList<String>();
		for (ControlFile controlFile : controlFiles) {
			dataFiles.addAll(controlFile.getDataFileNames());
		}
	}

	public void start() throws IOException, SQLException {
			
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

	private void copyDataFilesToHDFS(FileSystem local, FileSystem hdfs) throws IOException {
		
		//Copy all files into HDFS
		long totalSize = 0;
		for (String file : dataFiles) {
			Path path = new Path(file);
			
			long length = 0;
			try{
				length = local.getFileStatus(path).getLen();
			}catch(Exception e){}
			
			try{
				hdfs.copyFromLocalFile(path, stagingHDFSDirectory);

				totalSize += length;
				
				LOG.debug("the local file " + path + " (" + length 
						+ " bytes) has been copied to " + stagingHDFSDirectory);
			}catch(Exception e){
				LOG.error("the local file " + path + " could not be copied to HDFS", e);
				
				throw new IOException(e);
			}
		}
		
		LOG.info(dataFiles.size() + " files " + "("+ (totalSize / 1024 / 1024) 
				+ " MB) have been copied to HDFS");
	}

	/**
	 * Remove control file and staging data
	 * @throws FatalException 
	 * @throws SQLException 
	 * @throws IOException 
	 * 
	 * @throws Exception
	 */
	public void clean() throws FatalException, SQLException, IOException  {

		//Delete control files
		for (ControlFile controlFile : controlFiles) {
			try{
				controlFile.delete();
				
				LOG.debug("control file " + controlFile + " has been deleted");
			}catch(Exception e){
				LOG.error("the control file " + controlFile + " could not be deleted", e);
				LOG.error("the control file \"" + controlFile + "\" MUST be deleted before"
						+ " starting again the loader, otherwise data will be "
						+ " reinserted into final table (duplicates)");
				
				throw new FatalException(e);
			}
		}
		
		//Delete all local data files
		for (String file : dataFiles) {
			Path path = new Path(file);
			
			try {
				if(local.delete(path, true)){
					LOG.debug(file + " has been deleted");
				}else{
					throw new IOException();
				}
			} catch (IOException e) {
				LOG.error("the data file " + file + " could not be deleted", e);
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
			
			throw new IOException(e);
		}
		
		LOG.info("deleted staging data");
	}

}
