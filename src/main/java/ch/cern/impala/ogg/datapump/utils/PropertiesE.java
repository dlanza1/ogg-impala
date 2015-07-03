package ch.cern.impala.ogg.datapump.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import ch.cern.impala.ogg.datapump.impala.descriptors.ColumnDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.PartitioningColumnDescriptor;
import ch.cern.impala.ogg.datapump.oracle.ControlFile;
import ch.cern.impala.ogg.datapump.oracle.FileFormatException;

import com.google.common.base.Preconditions;

public class PropertiesE extends Properties {
	private static final long serialVersionUID = 3733307414558688437L;
	
	private final static Logger LOG = Logger.getLogger(PropertiesE.class);

	public static final String DEFAULT_PROPETIES_FILE = "config.properties";
	
	private static final String OGG_DATA_FOLDERS = "ogg.data.folders";
	
	public static final String OGG_CONTROL_FILE_NAME = "ogg.control.file.name";
	public static final String OGG_DEFINITION_FILE_NAME = "ogg.definition.file.name";

	private static final String SECONDS_BETWEEN_BATCHES = "batch.between.sec";
	private static final int DEFAULT_SECONDS_BETWEEN_BATCHES = 30;
	
	private static final String SECONDS_AFTER_FAILURE = "loader.failure.wait";
	private static final int DEFAULT_SECONDS_AFTER_FAILURE = 60;

	private static final String IMPALA_STAGING_DIRECTORY = "impala.staging.table.directory";
	private static final String DEFAULT_STAGING_HDFS_DIRECTORY = "ogg/staging/";
	
	private static final String IMPALA_HOST = "impala.host";
	private static final String DEFAULT_IMPALA_HOST = "localhost";

	private static final String IMPALA_PORT = "impala.port";
	private static final String DEFAULT_IMPALA_PORT = "21050";

	public static final String IMPALA_TABLE_SCHEMA = "impala.table.schema";
	public static final String IMPALA_STAGING_TABLE_SCHEMA = "impala.staging.table.schema";
	
	public static final String IMPALA_TABLE_NAME = "impala.table.name";
	public static final String IMPALA_STAGING_TABLE_NAME = "impala.staging.table.name";

	/**
	 * Parameter that indicates the names of the customized columns
	 */
	public static final String CUSTOMIZED_COLUMNS_NAMES = "impala.table.columns.customize";
	
	/**
	 * Describe the parameters which configure the columns
	 */
	public static final String COLUMN_PREFIX = "impala.table.column.";
	
	/**
	 * Parameters that indicates the partitioning columns names
	 */
	public static final String PARTITIONING_COLUMNS_NAMES = "impala.table.partitioning.columns";
	
	/**
	 * Describe the parameters which configure a partitioning columns
	 */
	public static final String PARTITIONING_COLUMN_PREFIX = "impala.table.partitioning.column.";

	/**
	 * Describe the parameters which configure the data types
	 */
	public static final String NAME_SUFFIX = ".name";
	
	/**
	 * Describe the parameters which configure the data types
	 */
	public static final String DATATYPE_SUFFIX = ".datatype";
	
	/**
	 * Describe the parameters which configure the expressions
	 */
	public static final String EXPRESSION_SUFFIX = ".expression";
	
	private static final String CREATE_STAGING_TABLE_QUERY = "impala.staging.table.query.create";
	private static final String DROP_STAGING_TABLE_QUERY = "impala.staging.table.query.drop";
	private static final String INSERT_INTO_QUERY = "impala.table.query.insert";
	private static final String CREATE_TABLE_QUERY = "impala.table.query.create";
	
	public PropertiesE() throws IOException{
		this(DEFAULT_PROPETIES_FILE);
	}

	public PropertiesE(String propFileName) throws IOException{
		super();
 
		FileInputStream inputStream = new FileInputStream(propFileName);
 
		try {
			load(inputStream);
			
			LOG.trace("the properties has been loaded from " + propFileName);
		} catch (NullPointerException e) {
			LOG.error("the properties could not been loaded from " + propFileName);

			throw e;
		}

	}

	public LinkedList<ControlFile> getSourceContorlFiles(String schema, String table) 
			throws IllegalStateException, IOException {
		
		LinkedList<String> dataFolders = getSourceLocalDirectories();
		
		String sourceControlFile_prop = getProperty(OGG_CONTROL_FILE_NAME);
		
		if(sourceControlFile_prop == null){
			sourceControlFile_prop = schema + "." + table + "control"; 
			
			LOG.warn("the name of the control filse was not specified, "
					+ "so the default name will be used (" + sourceControlFile_prop + ")");
		}
		
		LinkedList<ControlFile> controlFiles = new LinkedList<ControlFile>();
		for (String dataFolder : dataFolders) {
			controlFiles.add(new ControlFile(dataFolder + "/" + sourceControlFile_prop));
		}
		
		return controlFiles;
	}
	
	public File getDefinitionFile() throws IllegalStateException, IOException {
		
		String sourceControlFile_prop = getProperty(OGG_DEFINITION_FILE_NAME);
		
		if(sourceControlFile_prop == null){
			String error_message = "the path of the definition file "
					+ "must be specified by " + OGG_DEFINITION_FILE_NAME; 
			
			LOG.error(error_message);
			throw new IllegalStateException(error_message);
		}
		
		return new File(sourceControlFile_prop);
	}

	/**
	 * Get the time in milliseconds between batches
	 * 
	 * @return Time in milliseconds between batches
	 */
	public long getTimeBetweenBatches() {
		int seconds_between_batches = DEFAULT_SECONDS_BETWEEN_BATCHES;
		try{
			seconds_between_batches = Integer.valueOf(getProperty(SECONDS_BETWEEN_BATCHES));
		}catch(Exception e){
			LOG.warn("the number of seconds between batches has been set "
					+ "to the default value (" + seconds_between_batches + " seconds)");
		}
		
		return seconds_between_batches * 1000;
	}
	
	/**
	 * Get the time in milliseconds that the loader should wait after a failure
	 * 
	 * @return Time in milliseconds after failure
	 */
	public long getTimeAfterFailure() {
		int seconds_between_batches = DEFAULT_SECONDS_AFTER_FAILURE;
		try{
			seconds_between_batches = Integer.valueOf(getProperty(SECONDS_AFTER_FAILURE));
		}catch(Exception e){
			LOG.warn("the number of seconds after a failure has been set "
					+ "to the default value (" + seconds_between_batches + " seconds)");
		}
		
		return seconds_between_batches * 1000;
	}

	public Path getStagingHDFSDirectory(String schema, String table) {
		return new Path(getProperty(IMPALA_STAGING_DIRECTORY, 
				DEFAULT_STAGING_HDFS_DIRECTORY + schema + "/" + table));
	}

	public LinkedList<String> getSourceLocalDirectories() {
		
		String prop = getProperty(OGG_DATA_FOLDERS);
		Preconditions.checkNotNull(prop, "the property " + OGG_DATA_FOLDERS + " must be specified");
		
		String[] values = prop.replaceAll("\\s+","").split(",");;
		LinkedList<String> dirs = new LinkedList<String>();
		for (String val : values)
			dirs.add(val);
		
		return dirs;
	}

	public String getImpalaHost() {
		return getProperty(IMPALA_HOST, DEFAULT_IMPALA_HOST);
	}

	public int getImpalaPort() {
		return Integer.valueOf(getProperty(IMPALA_PORT, DEFAULT_IMPALA_PORT));
	}

	public LinkedList<PartitioningColumnDescriptor> getPartitioningColumns() throws FileFormatException {
		LinkedList<PartitioningColumnDescriptor> partColumns = new LinkedList<PartitioningColumnDescriptor>();
		
		if(!containsKey(PARTITIONING_COLUMNS_NAMES))
			return partColumns;
		
		String[] names = getProperty(PARTITIONING_COLUMNS_NAMES).replaceAll("\\s+","").split(","); 
		
		for (String name : names) {
			String dataTypeProperty = PARTITIONING_COLUMN_PREFIX + name + DATATYPE_SUFFIX;
			String dataType = getProperty(dataTypeProperty);
			if(dataType == null){
				FileFormatException fileFormatException = new FileFormatException(
						"the data type for the partitioning column " + name
						+ " must be specified with the parameter " + dataTypeProperty);
				LOG.error(fileFormatException.getMessage(), fileFormatException);
				throw fileFormatException;
			}
			
			String expressionProperty = PARTITIONING_COLUMN_PREFIX + name + EXPRESSION_SUFFIX;
			String expression = getProperty(expressionProperty);
			if(expression == null){
				FileFormatException fileFormatException = new FileFormatException(
						"the expression for the partitioning column " + name
						+ " must be specified with the parameter " + expressionProperty);
				LOG.error(fileFormatException.getMessage(), fileFormatException);
				throw fileFormatException;
			}
			
			partColumns.add(new PartitioningColumnDescriptor(name, dataType, expression));
		}
		
		return partColumns;
	}
	
	public HashMap<String, ColumnDescriptor> getCustomizedColumns() throws FileFormatException {
		HashMap<String, ColumnDescriptor> customColumns = new HashMap<String, ColumnDescriptor>();
		
		if(!containsKey(CUSTOMIZED_COLUMNS_NAMES))
			return customColumns;

		String[] names = getProperty(CUSTOMIZED_COLUMNS_NAMES).replaceAll("\\s+","").split(","); 
		
		for (String name : names) {
			String newName = getProperty(COLUMN_PREFIX + name + NAME_SUFFIX);
			String dataType = getProperty(COLUMN_PREFIX + name + DATATYPE_SUFFIX);
			String expression = getProperty(COLUMN_PREFIX + name + EXPRESSION_SUFFIX);

			customColumns.put(name, new ColumnDescriptor(newName, dataType, expression));
		}
		
		return customColumns;
	}

	public String getCreateStagingTableQuery() {
		return getProperty(CREATE_STAGING_TABLE_QUERY);
	}

	public String getInsertIntoQuery() {
		return getProperty(INSERT_INTO_QUERY);
	}

	public String getCreateTableQuery() {
		return getProperty(CREATE_TABLE_QUERY);
	}

	public String getDropStagingTableQuery() {
		return getProperty(DROP_STAGING_TABLE_QUERY);
	}
}
