package ch.cern.impala.ogg.datapump;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.impala.ImpalaClient;
import ch.cern.impala.ogg.datapump.impala.Query;
import ch.cern.impala.ogg.datapump.impala.QueryBuilder;
import ch.cern.impala.ogg.datapump.impala.descriptors.StagingTableDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.TableDescriptor;
import ch.cern.impala.ogg.datapump.oracle.ControlFile;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class ImpalaDataLoader {

	final private static Logger LOG = LoggerFactory.getLogger(ImpalaDataLoader.class);

	/**
	 * Milliseconds between batches
	 */
	private long ms_between_batches;	

	/**
	 * Control files which contains the name of the data files
	 */
	private LinkedList<ControlFile> sourceControlFiles;

	/**
	 * Local file system
	 */
	private LocalFileSystem local;
	
	/**
	 * Hadoop Distributed File System
	 */
	private FileSystem hdfs;

	/**
	 * Staging directory where the data will be stored temporally
	 */
	private Path stagingHDFSDirectory;

	/**
	 * Query to create the staging (temporal) table
	 */
	private Query createStagingTable;
	
	/**
	 * Query to delete the staging table
	 */
	private Query dropStagingTable;
	
	/**
	 * Query to insert staging data into final table
	 */
	private Query insertInto;
	
	/**
	 * Query to create the final table
	 */
	private Query createTargetTable;

	private ImpalaClient impalaClient;

	public ImpalaDataLoader(PropertiesE prop) 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException {
		
		// Get file systems
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		hdfs = FileSystem.get(conf);
		conf.set("fs.file.impl", LocalFileSystem.class.getName());
		local = FileSystem.getLocal(conf);
		
		// Get Impala client
		impalaClient = new ImpalaClient(prop.getImpalaHost(), prop.getImpalaPort());
		
		// We can run the loader either configuring the definition file path 
		// or configuring all the necessary queries 
		
		// Check if the path to the definition file was specified
		if(prop.containsKey(PropertiesE.OGG_DEFINITION_FILE_NAME)){
			// If so, create tables form definition file and
			// apply custom configuration
			
			configureFromDefinitionFile(prop, impalaClient);
		}else{
			
			// Otherwise, extract all the queries from the configuration
			configureWithoutDefinitionFile(prop, impalaClient);
		}
	
		LOG.info("query to create staging table set to: " + createStagingTable);
		LOG.info("query to drop staging table set to: " + dropStagingTable);
		LOG.info("insert query set to: " + insertInto);
		LOG.info("create target table query set to: " + createTargetTable);
		LOG.info("reading control data from " + sourceControlFiles);
		
		// Configure period of time for checking new data
		ms_between_batches = prop.getTimeBetweenBatches();
		
	}

	private void configureFromDefinitionFile(PropertiesE prop, ImpalaClient impalaClient)
			throws IllegalStateException, IOException, CloneNotSupportedException {

		// Get source table descriptor
		TableDescriptor sourceTableDes = TableDescriptor.createFromFile(prop.getDefinitionFile());
		LOG.debug("source " + sourceTableDes.toString());

		// Apply custom configuration to get the target table descriptor
		TableDescriptor targetTableDes = (TableDescriptor) sourceTableDes.clone();
		targetTableDes.applyCustomConfiguration(prop);

		// Get staging table descriptor
		StagingTableDescriptor stagingTableDes = sourceTableDes.getDefinitionForStagingTable();
		stagingTableDes.applyCustomConfiguration(prop);
		
		// Perform test on staging directory
		stagingHDFSDirectory = prop.getStagingHDFSDirectory(
								targetTableDes.getSchemaName(), targetTableDes.getTableName());
		// Get absolute path
		if(!stagingHDFSDirectory.isAbsolute())
			stagingHDFSDirectory = testStagingDirectory(hdfs, stagingHDFSDirectory);
		
		QueryBuilder queryBuilder = impalaClient.getQueryBuilder();

		// Get query for creating staging table
		String createStagingTableQuery_prop = prop.getCreateStagingTableQuery();
		if (createStagingTableQuery_prop == null) {
			createStagingTable = queryBuilder.createExternalTable(stagingTableDes, stagingHDFSDirectory);

			LOG.info("staging " + stagingTableDes);
		} else {
			createStagingTable = new Query(createStagingTableQuery_prop, impalaClient);
			LOG.info("query to create staging table set to: " + createStagingTable);
		}
		
		// Get query for dropping staging table
		String dropStagingTableQuery_prop = prop.getDropStagingTableQuery();
		if (dropStagingTableQuery_prop == null) {
			dropStagingTable = queryBuilder.dropTable(stagingTableDes);
		} else {
			dropStagingTable = new Query(dropStagingTableQuery_prop, impalaClient);
			LOG.info("query to drop staging table set to: " + dropStagingTable);
		}

		// Get query for importing data from staging table to final table
		String insertIntoQuery_prop = prop.getInsertIntoQuery();
		if (insertIntoQuery_prop == null) {
			insertInto = queryBuilder.insertInto(stagingTableDes, targetTableDes);
		} else {
			insertInto = new Query(insertIntoQuery_prop, impalaClient);
			LOG.info("insert query set to: " + insertInto);
		}

		// Get query for creating target table
		String createTargetTableQuery_prop = prop.getCreateTableQuery();
		if (createTargetTableQuery_prop == null) {
			createTargetTable = queryBuilder.createTable(targetTableDes);

			LOG.info("target " + targetTableDes);
		} else {
			createTargetTable = new Query(createTargetTableQuery_prop, impalaClient);
			LOG.info("create target table query set to: " + createTargetTable);
		}
		
		// Get control file which is generated by OGG
		sourceControlFiles = prop.getSourceContorlFiles(stagingTableDes.getSchemaName(),
														stagingTableDes.getTableName());
	}

	private void configureWithoutDefinitionFile(PropertiesE prop, ImpalaClient impalaClient) throws IOException {

		// Check all configuration needed
		String createStagingTableQuery_prop = prop.getCreateStagingTableQuery();
		String dropStagingTableQuery_prop = prop.getDropStagingTableQuery();
		String insertIntoQuery_prop = prop.getInsertIntoQuery();
		String createTargetTableQuery_prop = prop.getCreateTableQuery();

		if (createStagingTableQuery_prop == null
				|| dropStagingTableQuery_prop == null
				|| insertIntoQuery_prop == null
				|| createTargetTableQuery_prop == null
				|| prop.containsKey(PropertiesE.OGG_CONTROL_FILE_NAME) == false) {

			IllegalStateException e = new IllegalStateException(
					"the loader could be initialized"
							+ " because the configuration is not valid. You must specify either the"
							+ " definition file path, or the paameters for the four queries"
							+ " and the name of the control file.");

			LOG.error(e.getMessage(), e);
			throw e;
		}

		// Get query for creating staging table
		createStagingTable = new Query(createStagingTableQuery_prop,impalaClient);
		// Get query for dropping staging table
		dropStagingTable = new Query(dropStagingTableQuery_prop, impalaClient);
		// Get query for importing data from staging table to final table
		insertInto = new Query(insertIntoQuery_prop, impalaClient);
		// Get query for creating target table
		createTargetTable = new Query(createTargetTableQuery_prop, impalaClient);

		// Get staging directory
		stagingHDFSDirectory = prop.getStagingHDFSDirectory("", "");

		// Get control file
		sourceControlFiles = prop.getSourceContorlFiles(null, null);
	}
	
	private void start() throws Exception {
		
		// Get absolute path, test it and delete it
		stagingHDFSDirectory = testStagingDirectory(hdfs, stagingHDFSDirectory);
		
		// Get Impala connected
		impalaClient.connect();
		
		// Create target table if it does not exist
		try {
			createTargetTable.exect();
			LOG.info("created final table");
		} catch (SQLException e) {			
			if (!e.getMessage().contains("Table already exists:")) {
				LOG.error("final table could not be created", e);
				throw e;
			}
		}
		
		// Delete staging table if it exists
		try{
			dropStagingTable.exect();
			LOG.info("deleted staging table");
		}catch(SQLException e){
			if(!e.getMessage().contains("Table does not exist:")){
				throw e;
			}
		}
		
		// Check periodically for new data
		while (true) {
			long startTime = System.currentTimeMillis();

			// Control files which contains the list of files to process in this batch
			LinkedList<ControlFile> controlFilesToProcess = new LinkedList<ControlFile>();
			for (ControlFile sourceControlFile : sourceControlFiles) {
				ControlFile controlFileToProcess = sourceControlFile.getControlFileToProcess();
				
				if(controlFileToProcess != null)
					controlFilesToProcess.add(controlFileToProcess);
			}

			if (controlFilesToProcess.size() > 0) {
				LOG.info("there is new data to process");

				Batch batch = new Batch(local, 
										hdfs, 
										controlFilesToProcess,
										stagingHDFSDirectory, 
										dropStagingTable,
										createStagingTable, 
										insertInto);
				batch.start();
				batch.clean();
			} else {
				LOG.info("there is no data to process");
			}

			waitForNextBatch(startTime);
		}
	}
	
	private void waitForNextBatch(long startTime) {
		long timeDiff = System.currentTimeMillis() - startTime;

		long leftTime = ms_between_batches - timeDiff;
		
		if(leftTime < 0)
			return;

		LOG.info("waiting " + (leftTime / 1000) + " seconds...");

		try {
			TimeUnit.MILLISECONDS.sleep(leftTime);
		} catch (InterruptedException e) {}
	}

	/**
	 * Check if the staging directory can be created and deleted (if it does not exist)
	 * 
	 * @param hdfs File system
	 * @param directory Future staging directory to check
	 * @return Resolved directory
	 * @throws IOException
	 */
	private Path testStagingDirectory(FileSystem hdfs, Path directory) throws IOException {
		
		Path stagingDirectory;
		
		// If the directory does not exist
		if (!hdfs.exists(directory)) {	

			// create directory
			try {
				if (!hdfs.mkdirs(directory)) {
					IllegalStateException e = new IllegalStateException(
							"target directory (" + directory + ") could not be created");
					LOG.error(e.getMessage(), e);
					throw e;
				}
			} catch (IOException e) {
				LOG.error("target directory (" + directory + ") could not be created", e);
				throw e;
			}
		}

		// resolve absolute path
		stagingDirectory = hdfs.resolvePath(directory);

		// delete directory
		try {
			if (!hdfs.delete(directory, true)) {
				IllegalStateException e = new IllegalStateException(
						"target directory (" + directory + ") could not be deleted");
				LOG.error(e.getMessage(), e);
				throw e;
			}
		} catch (IOException e) {
			LOG.error("target (" + directory + ") directory could not be deleted", e);
			throw e;
		}

		return stagingDirectory;
	}

	public static void main(String[] args) throws Exception{
		String prop_file = args == null || args.length != 1 || args[0] == null ? 
				PropertiesE.DEFAULT_PROPETIES_FILE : args[0];
		
		LOG.info("inicializing loader (properties file = " + prop_file + ")");

		// Load properties file
		PropertiesE prop = new PropertiesE(prop_file);
		
		//Create and start loader
		ImpalaDataLoader loader = new ImpalaDataLoader(prop);
		
		// Configure period of time to wait in case of failure
		long ms_after_failure = prop.getTimeAfterFailure();
		
		while(true){
			try {
				loader.start();
			} catch (Exception e) {
				
				// Depending of the problem we should start again 
				// the loader (default behaviour) or finish the execution
				
				if (e instanceof NullPointerException
						|| e instanceof FileNotFoundException) {
					throw e;
				}
				
				LOG.error("there was an error in the current batch. "
						+ "Waiting " + (ms_after_failure / 1000) + " seconds before restarting the loader. "
						+ "Cause of error:", e);
				try {
					TimeUnit.MILLISECONDS.sleep(ms_after_failure);
				} catch (InterruptedException eSleep) {}
			}
		}
	}
	
}