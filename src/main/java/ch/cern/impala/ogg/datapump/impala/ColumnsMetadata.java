package ch.cern.impala.ogg.datapump.impala;

import java.util.LinkedList;
import java.util.List;

public class ColumnsMetadata {
	
	List<ColumnMetadata> columns;
	
	public ColumnsMetadata() {
		columns = new LinkedList<ColumnMetadata>();

		columns.add(new ColumnMetadata("VARIABLE_ID", "NUMBER"));
		columns.add(new ColumnMetadata("UTC_STAMP", "TIMESTAMP"));
		columns.add(new ColumnMetadata("VALUE", "DECIMAL"));
	}

	public List<ColumnMetadata> getColumns() {
		return columns;
	}

	public String asSQL() {
		String out = "(";
		
		for (ColumnMetadata column : columns) {
			out = out.concat(column.getName() + " STRING, ");
		}
		
		return out.substring(0, out.length() - 2) + ")";
	}
}
