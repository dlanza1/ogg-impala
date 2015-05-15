package ch.cern.impala.ogg.datapump.impala;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query {
	final private static Logger LOG = LoggerFactory.getLogger(Query.class);
	
	private String statement;
	private ImpalaClient client;
	
	public Query(String statement, ImpalaClient client) {
		this.statement = statement;
		this.client = null;
		
		LOG.debug("new query: " + this.statement);
	}

	public String getStatement() {
		return statement;
	}

	public void exect() throws SQLException {
		LOG.debug("executed query: " + this);
		
		client.exect(this);
	}
	
	@Override
	public String toString() {
		return statement;
	}

}