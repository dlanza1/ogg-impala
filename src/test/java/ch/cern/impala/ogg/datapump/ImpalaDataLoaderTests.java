package ch.cern.impala.ogg.datapump;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import ch.cern.impala.ogg.datapump.oracle.ControlFile;
import ch.cern.impala.ogg.datapump.utils.BadConfigurationException;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class ImpalaDataLoaderTests {
	
	@Test(expected=BadConfigurationException.class) //no configuration
	public void notValidConfiguration() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		new ImpalaDataLoader(prop);
		
	}

	@Test(expected=FileNotFoundException.class)
	public void definitionFileNotFound() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn("path/to/def/file");
		
		new ImpalaDataLoader(prop);
	}
	
	@Test(expected=BadConfigurationException.class) //data folders must be specified by ogg.data.folders property
	public void configureFromDefinitionFileWithDataFoldersNotSpecified() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn("src/test/resources/lhclog.def");
		
		new ImpalaDataLoader(prop);
	}
	
	@Test
	public void configureFromDefinitionFile() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn("src/test/resources/lhclog.def");
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DATA_FOLDERS)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("data1,data2");
		
		ImpalaDataLoader loader = new ImpalaDataLoader(prop);
		
		Assert.assertEquals(PropertiesE.DEFAULT_SECONDS_BETWEEN_BATCHES * 1000, loader.ms_between_batches);
		
		String current_path = Path.getPathWithoutSchemeAndAuthority(loader.stagingHDFSDirectory).toString();
		int pos = current_path.indexOf(PropertiesE.DEFAULT_STAGING_HDFS_DIRECTORY);
		Assert.assertEquals(PropertiesE.DEFAULT_STAGING_HDFS_DIRECTORY + "/LHCLOG/DATA_NUMERIC", 
				current_path.substring(pos, current_path.length()));
		
		Assert.assertEquals("CREATE EXTERNAL TABLE LHCLOG.DATA_NUMERIC_staging "
				+ "(VARIABLE_ID STRING, UTC_STAMP STRING, VALUE STRING) "
				+ "STORED AS textfile "
				+ "LOCATION '" + current_path + "'", 
				loader.createStagingTable.getStatement());
		
		Assert.assertEquals("CREATE TABLE LHCLOG.DATA_NUMERIC "
				+ "(VARIABLE_ID DOUBLE, UTC_STAMP TIMESTAMP, VALUE DOUBLE) "
				+ "STORED AS parquet", 
				loader.createTargetTable.getStatement());
		
		Assert.assertEquals("DROP TABLE LHCLOG.DATA_NUMERIC_staging", 
				loader.dropStagingTable.getStatement());

		Assert.assertEquals("INSERT INTO LHCLOG.DATA_NUMERIC "
				+ "SELECT cast(VARIABLE_ID as DOUBLE), cast(UTC_STAMP as TIMESTAMP), cast(VALUE as DOUBLE) "
				+ "FROM LHCLOG.DATA_NUMERIC_staging", 
				loader.insertInto.getStatement());
		
		LinkedList<ControlFile> scf = loader.sourceControlFiles;
		Assert.assertEquals("data1/LHCLOG.DATA_NUMERICcontrol", scf.get(0).toString());
		Assert.assertEquals("data2/LHCLOG.DATA_NUMERICcontrol", scf.get(1).toString());
	}
	
	@Test
	public void configureFromDefinitionFileCustomizingQueries() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DEFINITION_FILE_NAME)).thenReturn("src/test/resources/lhclog.def");
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DATA_FOLDERS)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("data1,data2");
		
		Mockito.when(prop.containsKey(PropertiesE.CREATE_STAGING_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CREATE_STAGING_TABLE_QUERY)).thenReturn("createStagingTableQuery");
		Mockito.when(prop.containsKey(PropertiesE.CREATE_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CREATE_TABLE_QUERY)).thenReturn("createTargetTableQuery");
		Mockito.when(prop.containsKey(PropertiesE.DROP_STAGING_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.DROP_STAGING_TABLE_QUERY)).thenReturn("dropStagingTableQuery");
		Mockito.when(prop.containsKey(PropertiesE.INSERT_INTO_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.INSERT_INTO_QUERY)).thenReturn("insertIntoQuery");
		
		ImpalaDataLoader loader = new ImpalaDataLoader(prop);
		
		Assert.assertEquals("createStagingTableQuery", loader.createStagingTable.getStatement());
		Assert.assertEquals("createTargetTableQuery", loader.createTargetTable.getStatement());
		Assert.assertEquals("dropStagingTableQuery", loader.dropStagingTable.getStatement());
		Assert.assertEquals("insertIntoQuery", loader.insertInto.getStatement());
	}
	
	@Test
	public void configureWithoutDefinitionFile() 
			throws IOException, IllegalStateException, CloneNotSupportedException, ClassNotFoundException, BadConfigurationException{
		
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_CONTROL_FILE_NAME)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_CONTROL_FILE_NAME)).thenReturn("SCHEMA.NAMEcontrol");
		try{
			new ImpalaDataLoader(prop);
			Assert.fail();
		}catch(BadConfigurationException e){}
		
		Mockito.when(prop.containsKey(PropertiesE.CREATE_STAGING_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CREATE_STAGING_TABLE_QUERY)).thenReturn("createStagingTableQuery");
		try{
			new ImpalaDataLoader(prop);
			Assert.fail();
		}catch(BadConfigurationException e){}
		
		Mockito.when(prop.containsKey(PropertiesE.DROP_STAGING_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.DROP_STAGING_TABLE_QUERY)).thenReturn("dropStagingTableQuery");
		try{
			new ImpalaDataLoader(prop);
			Assert.fail();
		}catch(BadConfigurationException e){}
		
		Mockito.when(prop.containsKey(PropertiesE.INSERT_INTO_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.INSERT_INTO_QUERY)).thenReturn("insertIntoQuery");
		try{
			new ImpalaDataLoader(prop);
		}catch(BadConfigurationException e){
			//data folders must be specified
		}
		
		Mockito.when(prop.containsKey(PropertiesE.OGG_DATA_FOLDERS)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("data1, data2");
		
		ImpalaDataLoader loader = new ImpalaDataLoader(prop);
		
		Assert.assertEquals(PropertiesE.DEFAULT_STAGING_HDFS_DIRECTORY, loader.stagingHDFSDirectory.toString());
		
		LinkedList<ControlFile> scf = loader.sourceControlFiles;
		Assert.assertEquals("data1/SCHEMA.NAMEcontrol", scf.get(0).toString());
		Assert.assertEquals("data2/SCHEMA.NAMEcontrol", scf.get(1).toString());
		
		Assert.assertEquals("createStagingTableQuery", loader.createStagingTable.getStatement());
		Assert.assertNull(loader.createTargetTable); //Does not need to be specified
		Assert.assertEquals("dropStagingTableQuery", loader.dropStagingTable.getStatement());
		Assert.assertEquals("insertIntoQuery", loader.insertInto.getStatement());
		
		Mockito.when(prop.containsKey(PropertiesE.CREATE_TABLE_QUERY)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CREATE_TABLE_QUERY)).thenReturn("createTargetTableQuery");
		
		loader = new ImpalaDataLoader(prop);
		
		Assert.assertEquals("createStagingTableQuery", loader.createStagingTable.getStatement());
		Assert.assertEquals("createTargetTableQuery", loader.createTargetTable.getStatement());
		Assert.assertEquals("dropStagingTableQuery", loader.dropStagingTable.getStatement());
		Assert.assertEquals("insertIntoQuery", loader.insertInto.getStatement());
	}
	
}