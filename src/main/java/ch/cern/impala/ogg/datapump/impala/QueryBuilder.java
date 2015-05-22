package ch.cern.impala.ogg.datapump.impala;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import ch.cern.impala.ogg.datapump.impala.descriptors.ColumnDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.StagingTableDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.TableDescriptor;

public class QueryBuilder {

	private ImpalaClient client;
	
	QueryBuilder(ImpalaClient impalaClient) {
		this.client = impalaClient;
	}

	public Query createExternalTable(TableDescriptor des, Path dir) {
		StringBuilder stmnt = new StringBuilder();
		
		stmnt.append("CREATE EXTERNAL TABLE ");
		stmnt.append(des.getSchemaName() + "." + des.getTableName());
		
		stmnt.append(" (");
		boolean first = true;
		for (ColumnDescriptor colDes : des.getColumnDefinitions()) {
			if(!first)
				stmnt.append(", ");
			first = false;
			
			stmnt.append(colDes.getName());
			stmnt.append(" ");
			stmnt.append(colDes.getType());
		}
		stmnt.append(") ");
		
		stmnt.append("STORED AS textfile ");
		stmnt.append("LOCATION '" + Path.getPathWithoutSchemeAndAuthority(dir) + "'");
		
		return new Query(stmnt.toString(), client);
	}
	
	public Query insertInto(StagingTableDescriptor sourceDes, TableDescriptor targetDes) {
		StringBuilder stmnt = new StringBuilder();
		
		//TODO set PARQUET_FILE_SIZE=64m;
		
		ArrayList<ColumnDescriptor> partitioningColumns = targetDes.getPartitioningColumnDefinitions();
		
		stmnt.append("INSERT INTO ");
		stmnt.append(targetDes.getSchemaName() + "." + targetDes.getTableName());
		
		if(partitioningColumns.size() > 0){
			stmnt.append(" PARTITION (");
			
			boolean first = true;
			for (ColumnDescriptor colDes : partitioningColumns) {
				if(!first)
					stmnt.append(", ");
				first = false;
				
				stmnt.append(colDes.getName());
			}
			
			stmnt.append(")");
		}
		
		stmnt.append(" SELECT ");
		
		boolean first = true;
		for (ColumnDescriptor colDes : targetDes.getColumnDefinitions()) {
			if(!first)
				stmnt.append(", ");
			first = false;
			
			stmnt.append(colDes.getExpression());
		}
		for (ColumnDescriptor colDes : partitioningColumns) {
			stmnt.append(", ");
			stmnt.append(colDes.getExpression());
		}
		
		stmnt.append(" FROM ");
		stmnt.append(sourceDes.getSchemaName() + "." + sourceDes.getTableName());
		
		return new Query(stmnt.toString(), client);
	}

	public Query createTable(TableDescriptor des) {
		StringBuilder stmnt = new StringBuilder();
		
		stmnt.append("CREATE TABLE ");
		stmnt.append(des.getSchemaName() + "." + des.getTableName());
		stmnt.append(" (");
		boolean first = true;
		for (ColumnDescriptor colDes : des.getColumnDefinitions()) {
			if(!first)
				stmnt.append(", ");
			first = false;
			
			stmnt.append(colDes.getName());
			stmnt.append(" ");
			stmnt.append(colDes.getType());
		}
		stmnt.append(") ");
		
		ArrayList<ColumnDescriptor> partitioningColumns = des.getPartitioningColumnDefinitions();
		if(partitioningColumns.size() > 0){
			stmnt.append("PARTITIONED BY (");
			
			first = true;
			for (ColumnDescriptor colDes : partitioningColumns) {
				if(!first)
					stmnt.append(", ");
				first = false;
				
				stmnt.append(colDes.getName());
				stmnt.append(" ");
				stmnt.append(colDes.getType());
			}
			
			stmnt.append(") ");
		}
		
		stmnt.append("STORED AS parquet");
		
		return new Query(stmnt.toString(), client);
	}

	public Query dropTable(TableDescriptor des) {
		String stmnt = "DROP TABLE " + des.getSchemaName() + "." + des.getTableName(); 
		
		return new Query(stmnt, client);
	}

}
