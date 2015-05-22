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
	
	public ImpalaClient(String host, int port) throws ClassNotFoundException, SQLException{
		String connection_url = "jdbc:hive2://" + host + ':' + port + "/;auth=noSasl";

		Class.forName(JDBC_DRIVER_NAME);

//		try {
//			con = DriverManager.getConnection(connection_url);
//		} catch (SQLException e) {
//			LOG.error(e.getMessage(), e);
//			
//			throw e;
//		}	
		
		LOG.debug("Impala client has been initialized (" + host + ":" + port + ")");
		
		queryBuilder = new QueryBuilder(this);
	}
	
	public void exect(Query query) throws SQLException{
		exect(query.getStatement());
	}
	
	public void exect(String statement) throws SQLException{
		Statement stmt = con.createStatement();
		stmt.execute(statement);
		stmt.close();
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
