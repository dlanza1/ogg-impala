package ch.cern.impala.ogg.datapump;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.impala.ImpalaClient;
import ch.cern.impala.ogg.datapump.oracle.ControlFile;
import ch.cern.impala.ogg.datapump.oracle.TableDefinition;

public class Batch {
	
	final private static Logger LOG = LoggerFactory.getLogger(Batch.class);

	//File systems
	private FileSystem local;
	private FileSystem hdfs;
	
	/**
	 * Source data control file
	 */
	private ControlFile controlFile;
	
	/**
	 * Impala client
	 */
	private ImpalaClient impC;
	
	//Staging data
	private Path stagingHDFSDirectory;
	private TableDefinition stagingTableDef;
	
	private TableDefinition targetTableDef;

	private List<String> datafiles;

	public Batch(FileSystem local, 
			FileSystem hdfs, 
			ControlFile controlFile, 
			ImpalaClient impC,
			Path stagingHDFSDirectory,
			TableDefinition stagingTableDef,
			TableDefinition targetTableDef) 
			throws IOException, ClassNotFoundException, SQLException {
		this.local = local;
		this.hdfs = hdfs;
		this.controlFile = controlFile;
		this.impC = impC;
		this.stagingHDFSDirectory = stagingHDFSDirectory;
		this.stagingTableDef = stagingTableDef;
		this.targetTableDef = targetTableDef;
	}

	public void start() throws Exception {
		
		if(controlFile.canDataBeLoadedIntoHDFS()){
			datafiles = controlFile.getDataFileNames();
			
			stagingHDFSDirectory = getStagingDirectory(hdfs, stagingHDFSDirectory);
		
			moveDataFilesToHDFS(local, hdfs, datafiles);
		}
		
		if(controlFile.canDataBeInsertedIntoFinalTable()){
			if(!stagingHDFSDirectory.isAbsolute())
				stagingHDFSDirectory = hdfs.resolvePath(stagingHDFSDirectory);
			
			impC.createExternalTable(stagingHDFSDirectory, stagingTableDef); 
			
			impC.insertoInto(stagingTableDef, targetTableDef);
			
			controlFile.markAsDataInsertedIntoFinalTable();
		}
	}

	private void moveDataFilesToHDFS(FileSystem local, FileSystem hdfs, List<String> files) throws Exception {
		
		//Copy all files
		long totalSize = 0;
		for (String file : files) {
			Path path = new Path(file);
			
			long length = 0;
			try{
				length = local.getFileStatus(path).getLen();
			}catch(IOException e){}
			
			try{
				hdfs.copyFromLocalFile(path, stagingHDFSDirectory);

				totalSize += length;
				
				LOG.debug("the local file " + path + " (" + length 
						+ " bytes) has been moved to " + stagingHDFSDirectory);
			}catch(Exception e){
				LOG.error("the local file " + path + " could not be copied to HDFS");
				
				throw e;
			}
		}
		
		//Delete all copied data files
		for (String file : files) {
			Path path = new Path(file);
			
			if(local.delete(path, true)){
				LOG.debug(file + " has been deleted");
			}else{
				throw new IllegalStateException("the data file " + file + " could not be deleted");
			}
		}
		
		controlFile.filesLoadedIntoHDFS();
		
		LOG.info(files.size() + " files " + "("+ totalSize + " bytes) have been moved to HDFS");
	}

	private Path getStagingDirectory(FileSystem hdfs, Path directory) throws IOException {
		
		if(hdfs.exists(directory)){
			if(!hdfs.delete(directory, true)){
				IllegalStateException e = new IllegalStateException("target directory could not be deleted");
				LOG.error(e.getMessage(), e);
				throw e;
			}
			
			LOG.warn("the directory " + directory + " had to be deleted in HDFS");
		}
		
		if(!hdfs.mkdirs(directory)){
			IllegalStateException e = new IllegalStateException("target directory could not be created");
			LOG.error(e.getMessage(), e);
			throw e;
		}
		
		return hdfs.resolvePath(directory);
	}

	public void clean() throws Exception {

		//Delete control file
		try{
			controlFile.delete();
		}catch(Exception e){
			LOG.error("the control file " + controlFile + " could not be deleted", e);
			
			throw e;
		}
		
		//Remove staging data in Impala and HDFS
		try{
			impC.drop(stagingTableDef);
		}catch(SQLException e){
			LOG.error("the Impala table " 
					+ stagingTableDef.getSchemaName() + "." + stagingTableDef.getTableName()
					+ " which contains the staging data could not be deleted", e);
			
			throw e;
		}
		try{
			hdfs.delete(stagingHDFSDirectory, true);
		}catch(Exception e){
			LOG.error("the HDFS directory " + stagingHDFSDirectory + " which contains "
					+ "the data of the staging table could not be deleted", e);
			
			throw e;
		}
	}

}
