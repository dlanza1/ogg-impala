package ch.cern.impala.ogg.datapump;

import java.util.LinkedList;
import java.util.List;

public class OTableMetadata {

	public String getSchemaName() {
		return "lhclog";
	}

	public String getTableName() {
		return "data_numeric_ogg";
	}

	public List<OColumn> getColumns() {
		List<OColumn> columns = new LinkedList<OColumn>();;
		
		columns.add(new OColumn("VARIABLE_ID", "NUMBER"));
		columns.add(new OColumn("UTC_STAMP", "TIMESTAMP"));
		columns.add(new OColumn("VALUE", "DECIMAL"));
		
		return columns;
	}

}
