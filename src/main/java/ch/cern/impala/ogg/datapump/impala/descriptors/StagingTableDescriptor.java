package ch.cern.impala.ogg.datapump.impala.descriptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.oracle.FileFormatException;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class StagingTableDescriptor extends TableDescriptor {
	
	final private static Logger LOG = LoggerFactory.getLogger(StagingTableDescriptor.class);

	public StagingTableDescriptor(String schema, String table) {
		super(schema, table);
	}

	@Override
	public void applyCustomConfiguration(PropertiesE prop) throws FileFormatException {

		if(prop.containsKey(PropertiesE.IMPALA_STAGING_TABLE_SCHEMA))
			schemaName = prop.getProperty(PropertiesE.IMPALA_STAGING_TABLE_SCHEMA);
		
		if(prop.containsKey(PropertiesE.IMPALA_STAGING_TABLE_NAME))
			tableName = prop.getProperty(PropertiesE.IMPALA_STAGING_TABLE_NAME);
		
		LOG.debug("the staging Impala table name will be: " + schemaName + "." + tableName);
	}
	
}
