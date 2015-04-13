package ch.cern.impala.ogg.datapump;

import java.util.LinkedList;
import java.util.List;

public class ColumnMetadata {
	
	List<Column> columns;
	
	public ColumnMetadata() {
		columns = new LinkedList<Column>();

		columns.add(new Column("VARIABLE_ID", "NUMBER"));
		columns.add(new Column("UTC_STAMP", "TIMESTAMP"));
		columns.add(new Column("VALUE", "DECIMAL"));
	}

	public List<Column> getColumns() {
		return columns;
	}

	public String asSQL() {
		String out = "(";
		
		for (Column column : columns) {
			out = out.concat(column.getName() + " STRING, ");
		}
		
		return out.substring(0, out.length() - 2) + ")";
	}
}
