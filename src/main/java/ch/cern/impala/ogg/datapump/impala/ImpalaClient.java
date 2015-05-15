package ch.cern.impala.ogg.datapump.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public void createTable(TableDefinition tableDef) throws SQLException {
		
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
	}

	public void close(){
		try {
			con.close();
		} catch (Exception e) {}
	}

	public void createExternalTable(Path dir, TableDefinition def) throws SQLException {
		try{
			exect("DROP TABLE " + def.getSchemaName() + "." + def.getTableName());
		}catch(Exception e){}
		
		String smnt = "CREATE EXTERNAL TABLE " + def.getSchemaName() + "." + def.getTableName()
								+ " (" + def.getColumnsAsSQL() + ")"
								+ " STORED AS textfile"
								+ " LOCATION '" + Path.getPathWithoutSchemeAndAuthority(dir) + "'";
		exect(smnt);
		
		LOG.info("created external table: " + def.getSchemaName() + "." + def.getTableName());
		LOG.debug(smnt);
	}

	public void insertoInto(TableDefinition sourceTab, TableDefinition targetTab) throws SQLException {
		
		String stmn = "INSERT INTO " + targetTab.getSchemaName() + "." + targetTab.getTableName()
							+ " SELECT " + targetTab.getExpressions()
							+ " FROM " + sourceTab.getSchemaName() + "." + sourceTab.getTableName();
		
		exect(stmn);
		
		LOG.info("inserted data into " + targetTab.getSchemaName() + "." + targetTab.getTableName() 
					+ " from " + sourceTab.getSchemaName() + "." + sourceTab.getTableName());
	}

	public void drop(TableDefinition def) throws SQLException {
		exect("DROP TABLE " + def.getSchemaName() + "." + def.getTableName());
		
		LOG.info("deleted table " + def.getSchemaName() + "." + def.getTableName());
	}
}
