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
	
	ImpalaClient impC;

	public Batch(ControlFile controlFile, PropertiesE prop) 
			throws IOException, ClassNotFoundException, SQLException {
		
		this.controlFile = controlFile;
		this.stagingHDFSDirectory = prop.getStagingHDFSDirectory();
		this.sourceLocalDirectory = prop.getSourceLocalDirectory();
		
		impC = new ImpalaClient(prop.getImpalaHost(), prop.getImpalaPort());
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
				
				LOG.debug("the local file " + path + " has been moved to " + stagingHDFSDirectory + " (HDFS");
			}catch(Exception e){
				LOG.error("the name of the file " + path 
						+ " is in the control file but the file could not be read");
				
				throw e;
			}
		}
		
		//Delete all copied files
		for (String file : files) {
			Path path = new Path(sourceLocalDirectory + "/" + file);
			
			boolean success = local.delete(path, true);
			
			if(success)
				LOG.debug(file + " has been deleted");
			else
				throw new IllegalStateException(file + " could not be deleted");
		}
		
		LOG.info(files.size() + " files have been moved to " + stagingHDFSDirectory + " (HDFS)");
		
		//Write label in control file to avoid re-load
		controlFile.markAsFilesLoadedIntoHDFS();
		
		if(controlFile.isMarkedAsFilesLoadedIntoHDFS()){
			OTableMetadata metadata = new OracleClient().getMetadata("table");
			
			//Create Impala staging table
			Path tableDir = hdfs.resolvePath(stagingHDFSDirectory);
			ITable externalTable = impC.createStagingTable(tableDir, metadata);
			
			//Get final table (the table is created if it does not exist)
			ITable finalTable = impC.createTable(metadata);
			
			//Insert staging data into final table
			externalTable.insertoInto(finalTable);
			
			//Write label in control file to avoid re-insert
			controlFile.markAsDataInsertedIntoFinalTable();
			
			//Remove staging data
			try{
				externalTable.drop();
			}catch(Exception e){
				LOG.error("the Impala table " + externalTable + " which contains "
						+ "the staging data could not be deleted");
			}
			try{
				hdfs.delete(tableDir, true);
			}catch(Exception e){
				LOG.error("the HDFS directory " + tableDir + " which contains "
						+ "the data of the staging table could not be deleted");
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
