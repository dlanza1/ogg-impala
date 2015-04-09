package ch.cern.impala.ogg.datapump;


public class OracleClient {

	public OracleClient(){
		
	}
	
	public OTableMetadata getMetadata(String table) {
		return new OTableMetadata();
	}

}
