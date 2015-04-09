package ch.cern.impala.ogg.datapump;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaClient {
	
	final private static Logger LOG = LoggerFactory.getLogger(Batch.class);

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private Connection con;
	
	public ImpalaClient(String host, int port) throws ClassNotFoundException, SQLException{
		String connection_url = "jdbc:hive2://" + host + ':' + port + "/;auth=noSasl";

		Class.forName(JDBC_DRIVER_NAME);

		try {
			con = DriverManager.getConnection(connection_url);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			
			throw e;
		}	
	}
	
	public void exect(String statement) throws SQLException{
		Statement stmt = con.createStatement();
		boolean resul = stmt.execute(statement);
		stmt.close();
	}

	public ITable createTable(OTableMetadata sourceTableMetadata) throws SQLException {
		List<OColumn> sourceTableColumns = sourceTableMetadata.getColumns();
		
		String schemaName = sourceTableMetadata.getSchemaName();
		String tableName = sourceTableMetadata.getTableName();
		
		String smnt = "CREATE TABLE " + schemaName + "." + tableName
								+ " " + getColumnsAsStringToSql(sourceTableColumns)
								+ " STORED AS parquet";
		
		try{
			exect(smnt);
			
			LOG.info("created final table");
			LOG.debug(smnt);
		}catch(SQLException e){
			if(!e.getMessage().contains("Table already exists:")){
				LOG.error("final table could not be created", e);
				
				throw e;
			}
		}
								
		return new ITable(this, sourceTableMetadata);	
	}
	
	public ITable createStagingTable(Path tableDir, OTableMetadata sourceTableMetadata) throws SQLException {
		List<OColumn> sourceTableColumns = sourceTableMetadata.getColumns();
		
		String schemaName = sourceTableMetadata.getSchemaName();
		String tableName = sourceTableMetadata.getTableName().concat("_staging");
		
		try{
			exect("DROP TABLE " + schemaName + "." + tableName);
		}catch(Exception e){}
		
		String smnt = "CREATE EXTERNAL TABLE " + schemaName + "." + tableName
								+ " " + getColumnsAsStringToSql(sourceTableColumns)
								+ " STORED AS textfile"
								+ " LOCATION '" + Path.getPathWithoutSchemeAndAuthority(tableDir) + "'";
		
		exect(smnt);
		
		LOG.info("created external table");
		LOG.debug(smnt);
								
		return new ITable(this, sourceTableMetadata);
	}
	
	private String getColumnsAsStringToSql(List<OColumn> sourceTableColumns) {
		String out = "(";
		
		for (OColumn column : sourceTableColumns) {
			out = out.concat(column.getName() + " STRING, ");
		}
		
		return out.substring(0, out.length() - 2) + ")";
	}

	public boolean exist(String schemaName, String tableName) {
		return false;
	}

	public void close(){
		try {
			con.close();
		} catch (Exception e) {
		}
	}
}
