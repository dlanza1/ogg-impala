package ch.cern.impala.ogg.datapump.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.oracle.ColumnDefinition;
import ch.cern.impala.ogg.datapump.oracle.TableDefinition;

public class ImpalaClient {
	
	final private static Logger LOG = LoggerFactory.getLogger(ImpalaClient.class);

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
		
		stmt.execute(statement);
		
		stmt.close();
	}

	public ITable createTable(TableDefinition tableDef) throws SQLException {
		
		String schema = tableDef.getSchemaName();
		String name = tableDef.getTableName();
		
		String smnt = "CREATE TABLE " + schema + "." + name
								+ " (" + tableDef.getColumnsAsSQL() + ")"
								+ " STORED AS parquet";
		
		try{
			exect(smnt);
			
			LOG.info("created final table: " + schema + "." + name);
			LOG.debug(smnt);
		}catch(SQLException e){
			if(!e.getMessage().contains("Table already exists:")){
				LOG.error("final table could not be created", e);
				
				throw e;
			}
		}
								
		return new ITable(this, tableDef);	
	}
	
	public ITable createExternalTable(String schema, String name, Path tableDir, TableDefinition tableDef) throws SQLException {
		
		//Create new table definition with STRING data types and new names
		TableDefinition newTabDef = new TableDefinition(schema, name);
		for(ColumnDefinition col : tableDef.getColumnsDefinitions()){
			ColumnDefinition newCol = col.clone();
			newCol.setType("STRING");
			newTabDef.addColumnDefinition(newCol);
		}
		
		try{
			exect("DROP TABLE " + schema + "." + name);
		}catch(Exception e){}
		
		String smnt = "CREATE EXTERNAL TABLE " + schema + "." + name
								+ " (" + newTabDef.getColumnsAsSQL() + ")"
								+ " STORED AS textfile"
								+ " LOCATION '" + Path.getPathWithoutSchemeAndAuthority(tableDir) + "'";
		exect(smnt);
		
		LOG.info("created external table: " + schema + "." + name);
		LOG.debug(smnt);
		
		return new ITable(this, newTabDef);
	}

	public void close(){
		try {
			con.close();
		} catch (Exception e) {}
	}
}
