package ch.cern.impala.ogg.datapump;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class PropertiesE extends Properties {
	private static final long serialVersionUID = 3733307414558688437L;
	
	private final static Logger LOG = Logger.getLogger(PropertiesE.class);

	public static final String DEFAULT_PROPETIES_FILE = "config.properties";
	
	public static final String OGG_DATA_FOLDER = "ogg.data.folder";
	
	public static final String OGG_CONTROL_FILE_NAME = "ogg.control.file.name";

	private static final int DEFAULT_SECONDS_BETWEEN_BATCHES = 10;
	public static final String SECONDS_BETWEEN_BATCHES = "batch.between.sec";

	private static final String STAGING_HDFS_DIRECTORY = "hdfs.staging.directory";
	private static final String DEFAULT_STAGING_HDFS_DIRECTORY = "ogg/staging";

	private static final String DEFAULT_IMPALA_HOST = "localhost";
	private static final String IMPALA_HOST = "impala.host";

	private static final String IMPALA_PORT = "impala.port";
	private static final String DEFAULT_IMPALA_PORT = "21050"; 
	
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

	public ControlFile getSourceContorlFile() throws IllegalStateException {
		String oggDataFolder_prop = getProperty(OGG_DATA_FOLDER);
		String sourceControlFile_prop = getProperty(OGG_CONTROL_FILE_NAME);
		if(oggDataFolder_prop == null || sourceControlFile_prop == null){
			LOG.error("the path of the control file should be specified");
			
			throw new IllegalStateException("the path of the control file should be specified");
		}
		
		return new ControlFile(oggDataFolder_prop + "/" +sourceControlFile_prop);
	}

	public long getMsBetweenBatches() {
		int seconds_between_batches = DEFAULT_SECONDS_BETWEEN_BATCHES;
		try{
			seconds_between_batches = Integer.valueOf(getProperty(SECONDS_BETWEEN_BATCHES));
		}catch(Exception e){
			LOG.warn("the number of seconds between batches has been set "
					+ "to the default value (" + seconds_between_batches + " seconds)");
		}
		
		return seconds_between_batches * 1000;
	}

	public Path getStagingHDFSDirectory() {
		return new Path(getProperty(STAGING_HDFS_DIRECTORY, DEFAULT_STAGING_HDFS_DIRECTORY));
	}

	public String getSourceLocalDirectory() {
		return getProperty(OGG_DATA_FOLDER);
	}

	public String getImpalaHost() {
		return getProperty(IMPALA_HOST, DEFAULT_IMPALA_HOST);
	}

	public int getImpalaPort() {
		return Integer.valueOf(getProperty(IMPALA_PORT, DEFAULT_IMPALA_PORT));
	}

}
