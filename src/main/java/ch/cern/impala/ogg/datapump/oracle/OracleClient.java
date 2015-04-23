package ch.cern.impala.ogg.datapump.oracle;

import ch.cern.impala.ogg.datapump.impala.ColumnsMetadata;


public class OracleClient {

	public OracleClient(){
		
	}
	
	public ColumnsMetadata getMetadata(String table) {
		return new ColumnsMetadata();
	}

}
