package ch.cern.impala.ogg.datapump.utils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import ch.cern.impala.ogg.datapump.oracle.ControlFile;

@SuppressWarnings("serial")
public class PropertiesETests extends Properties {
	
	@Test
	public void mandatoryArguments() throws IOException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		// OGG_DATA_FOLDERS
		
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn(null);
		try {
			prop.getSourceLocalDirectories();
			Assert.fail();
		} catch (BadConfigurationException e) {}

		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("aa,bb");
		LinkedList<String> dirs = prop.getSourceLocalDirectories();
		Assert.assertEquals(dirs.get(0), "aa");
		Assert.assertEquals(dirs.get(1), "bb");
		
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("aa, bb");
		dirs = prop.getSourceLocalDirectories();
		Assert.assertEquals(dirs.get(0), "aa");
		Assert.assertEquals(dirs.get(1), "bb");
		
	}
	
	@Test
	public void numbers() throws IOException, BadConfigurationException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		// IMPALA_PORT
		
		Mockito.when(prop.getProperty(PropertiesE.IMPALA_PORT)).thenReturn("diez");
		try {
			prop.getImpalaPort();
			Assert.fail();
		} catch (BadConfigurationException e) {}
		
		Mockito.when(prop.getProperty(PropertiesE.IMPALA_PORT)).thenReturn("9823");
		Assert.assertEquals(9823, prop.getImpalaPort());
		
		Mockito.when(prop.getProperty(PropertiesE.IMPALA_PORT)).thenReturn(null);
		Assert.assertEquals(PropertiesE.DEFAULT_IMPALA_PORT, prop.getImpalaPort());
		
		// SECONDS_AFTER_FAILURE
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_AFTER_FAILURE)).thenReturn("diez");
		try {
			prop.getTimeAfterFailure();
			Assert.fail();
		} catch (BadConfigurationException e) {}
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_AFTER_FAILURE)).thenReturn("912");
		Assert.assertEquals(912000, prop.getTimeAfterFailure());
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_AFTER_FAILURE)).thenReturn(null);
		Assert.assertEquals(PropertiesE.DEFAULT_SECONDS_AFTER_FAILURE * 1000, prop.getTimeAfterFailure());
		
		// SECONDS_BETWEEN_BATCHES
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_BETWEEN_BATCHES)).thenReturn("diez");
		try {
			prop.getTimeBetweenBatches();
			Assert.fail();
		} catch (BadConfigurationException e) {}
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_BETWEEN_BATCHES)).thenReturn("773");
		Assert.assertEquals(773000, prop.getTimeBetweenBatches());
		
		Mockito.when(prop.getProperty(PropertiesE.SECONDS_BETWEEN_BATCHES)).thenReturn(null);
		Assert.assertEquals(PropertiesE.DEFAULT_SECONDS_BETWEEN_BATCHES * 1000, prop.getTimeBetweenBatches());
	}
	
	@Test
	public void controlFiles() throws IOException{
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		// When OGG_DATA_FOLDERS is null should fail
		try {
			prop.getSourceContorlFiles(null, null);
			
			Assert.fail();
		} catch (BadConfigurationException e) {
		} catch (IOException e) {
			Assert.fail();
		}
		
		// When arguments are null
		Mockito.when(prop.getProperty(PropertiesE.OGG_DATA_FOLDERS)).thenReturn("data1,data2");
		try {
			prop.getSourceContorlFiles(null, null);
			
			Assert.fail();
		} catch (NullPointerException e) {
		} catch (BadConfigurationException e) {
			Assert.fail();
		} catch (IOException e) {
			Assert.fail();
		}
		
		try {
			LinkedList<ControlFile> dirs = prop.getSourceContorlFiles("SCHEMA", "TABLE");
			
			Assert.assertEquals("data1/SCHEMA.TABLEcontrol", dirs.get(0).getPath());
			Assert.assertEquals("data2/SCHEMA.TABLEcontrol", dirs.get(1).getPath());
		} catch (BadConfigurationException e) {
			Assert.fail();
		} catch (IOException e) {
			Assert.fail();
		}

	}
	
}
