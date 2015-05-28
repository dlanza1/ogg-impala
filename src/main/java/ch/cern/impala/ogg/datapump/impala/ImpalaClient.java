package ch.cern.impala.ogg.datapump.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaClient {
	
	final private static Logger LOG = LoggerFactory.getLogger(ImpalaClient.class);

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	private Connection con;
	
	private QueryBuilder queryBuilder;

	private String connectionString;
	
	public ImpalaClient(String host, int port) throws ClassNotFoundException{
		connectionString = "jdbc:hive2://" + host + ':' + port + "/;auth=noSasl";

		Class.forName(JDBC_DRIVER_NAME);
		
		queryBuilder = new QueryBuilder(this);
	}
	
	public void connect() throws SQLException {
		try {
			con = DriverManager.getConnection(connectionString);
			
			LOG.debug("Impala client has been initialized (" + connectionString + ")");
		} catch (SQLException e) {
			LOG.error("the connection with the Impala daemon could not be established", e);
			
			throw e;
		}	
	}

	public void exect(Query query) throws SQLException{
		exect(query.getStatement());
	}
	
	public void exect(String statement) throws SQLException{
		LOG.debug("executing query: " + statement);
		
		Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(statement);
			stmt.close();
			
			LOG.debug("executed query: " + statement);
		} catch (SQLException e) {
			LOG.error("the following query could not be executed: " + statement, e);
			throw e;
		}
	}

	public void close(){
		try {
			con.close();
		} catch (Exception e) {}
	}

	public QueryBuilder getQueryBuilder() {
		return queryBuilder;
	}
	
}
