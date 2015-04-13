package ch.cern.impala.ogg.datapump;

import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ITable {
	
	final private static Logger LOG = LoggerFactory.getLogger(ITable.class);

	private ImpalaClient impalaClient;
	
	private String schema;
	
	private String name;
	
	private ColumnMetadata columnMetadata;

	public ITable(ImpalaClient impalaClient, String schema, String name, ColumnMetadata columnMetadata) {
		this.impalaClient = impalaClient;
		this.schema = schema;
		this.name = name;
		this.columnMetadata = columnMetadata;
	}

	public void insertoInto(ITable targetTable) throws SQLException {
		
		String stmn = "INSERT INTO " + targetTable.getSchema() + "." + targetTable.getName()
							+ " SELECT * FROM " + this.schema + "." + this.name;
		
		impalaClient.exect(stmn);
		
		LOG.info("inserted data into " + targetTable.getSchema() + "." + targetTable.getName() 
					+ " from " + this.schema + "." + this.name);
	}

	private String getSchema() {
		return schema;
	}

	public String getName() {
		return name;
	}

	public ColumnMetadata getColumnMetadata() {
		return columnMetadata;
	}
	
	public void drop() throws SQLException {
		impalaClient.exect("DROP TABLE " + schema + "." + name);
		
		LOG.info("deleted table " + schema + "." + name);
	}

	@Override
	public String toString() {
		return schema + "." + name;
	}

	public ITable createStagingTable(Path tableDir) throws SQLException {
		return impalaClient.createExternalTable(schema, name.concat("_staging"), tableDir, columnMetadata);
	}
}
