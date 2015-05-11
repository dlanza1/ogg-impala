package ch.cern.impala.ogg.datapump.impala;

import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.oracle.TableDefinition;

public class ITable {
	
	final private static Logger LOG = LoggerFactory.getLogger(ITable.class);

	private ImpalaClient impalaClient;

	private TableDefinition tableDef;

	public ITable(ImpalaClient impalaClient,  TableDefinition tableDef) {
		this.impalaClient = impalaClient;

		this.tableDef = tableDef;
	}

	public void insertoInto(ITable targetTable) throws SQLException {
		
		String stmn = "INSERT INTO " + targetTable.getSchema() + "." + targetTable.getName()
							+ " SELECT " + targetTable.getTableDefinition().getColumnsWithCastingAsSQL()
							+ " FROM " + getSchema() + "." + getName();
		
		impalaClient.exect(stmn);
		
		LOG.info("inserted data into " + targetTable.getSchema() + "." + targetTable.getName() 
					+ " from " + getSchema() + "." + getName());
	}

	private String getSchema() {
		return this.tableDef.getSchemaName();
	}

	public String getName() {
		return this.tableDef.getTableName();
	}

	public TableDefinition getTableDefinition() {
		return tableDef;
	}
	
	public void drop() throws SQLException {
		impalaClient.exect("DROP TABLE " + getSchema() + "." + getName());
		
		LOG.info("deleted table " + getSchema() + "." + getName());
	}

	@Override
	public String toString() {
		return getSchema() + "." + getName();
	}

	public ITable createStagingTable(Path tableDir) throws SQLException {
		return impalaClient.createExternalTable(getSchema(), getName().concat("_staging"), tableDir, tableDef);
	}
}
