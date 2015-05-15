package ch.cern.impala.ogg.datapump.oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.impala.TypeConverter;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

import com.google.common.base.Preconditions;

public class TableDefinition {

	final private static Logger LOG = LoggerFactory.getLogger(TableDefinition.class);

	/**
	 * The line of the definition file which indicates the name of the table
	 * must start with this prefix.
	 */
	private static String PREFIX_TABLE_NAME = "Definition for table ";
	
	/**
	 * The line of the definition file which indicates the number of columns
	 * must start with this prefix.
	 */
	private static String PREFIX_NUM_COLUMNS = "Columns: ";

	String schemaName;
	String tableName;

	/**
	 * List of columns (useful when we want the columns in order)
	 */
	ArrayList<ColumnDefinition> columns_list;
	
	/**
	 * Map of columns (useful when we want to find a column by the name)
	 */
	HashMap<String, ColumnDefinition> columns_map;
	
	String columnsAsSql = null;
	String columnsWithCastingAsSql = null;

	public TableDefinition(String schema, String table) {
		this.schemaName = schema;
		this.tableName = table;
		
		columns_list = new ArrayList<ColumnDefinition>();
		columns_map = new HashMap<String, ColumnDefinition>();
		
		LOG.debug("created table definition for schema=" + schema + " table=" + table);
	}

	public String getColumnsAsSQL() {
		
		if(columnsAsSql == null){
			for (ColumnDefinition columnDefinition : columns_list) {
				if(columnsAsSql == null){
					columnsAsSql = columnDefinition.getName() + " " + columnDefinition.getType(); 
				}else{
					columnsAsSql = columnsAsSql
							+ ", " + columnDefinition.getName() + " " + columnDefinition.getType();
				}
			}
		}
		
		return columnsAsSql;
	}
	
	/**
	 * Get a string with the expressions of all columns separated by comma
	 * that once applied produce the values of each column
	 * 
	 * @return the expressions of all columns
	 */
	public String getExpressions() {
		if(columnsWithCastingAsSql == null){
			for (ColumnDefinition columnDefinition : columns_list) {
				if(columnsWithCastingAsSql == null){
					columnsWithCastingAsSql = columnDefinition.getExpression(); 
				}else{
					columnsWithCastingAsSql = columnsWithCastingAsSql 
									+ ", " + columnDefinition.getExpression();
				}
			}
		}
		
		return columnsWithCastingAsSql;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public static TableDefinition create(PropertiesE prop) throws IOException {
		
		File definitionFile = prop.getDefinitionFile();

		LOG.info("reading table definition from " + definitionFile.getAbsolutePath());

		TableDefinition tableDef = null;

		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(definitionFile));
		String line;
		while ((line = br.readLine()) != null) {
			if(line.startsWith(PREFIX_TABLE_NAME)){
				
				//Get table name
				String identifier = line.replaceFirst(PREFIX_TABLE_NAME, "");

				String[] fields = identifier.split("\\.");
				String schema = fields[0];
				String table = fields[1];
				
				tableDef = new TableDefinition(schema, table);
			}else if(line.startsWith(PREFIX_NUM_COLUMNS)){
				
				//If it find the line which contains the number of rows before
				//the line of the table name, the format of the file is not correct
				if(tableDef == null){
					FileFormatException formatExcep = new FileFormatException(
							"the format of the definition file is not correct because "
							+ "the line starting with '" + PREFIX_TABLE_NAME + "' was not found.");
					LOG.error(formatExcep.getMessage(), formatExcep);
					throw formatExcep;
				}
				
				String num = line.replaceFirst(PREFIX_NUM_COLUMNS, "");
				
				int numColumns = Integer.valueOf(num);
				
				//Read columns metadata
				for (int c = 0; c < numColumns; c++) {
					line = br.readLine();
					String[] fields = line.replaceAll("\\s+", " ").split(" ");
					
					//Get required columns
					String name = fields[0]; 
					
					//Get Impala column type
					int jdbcType = Integer.valueOf(fields[19]);
					String type = TypeConverter.toImpalaType(jdbcType).toString();
					
					//Create column definition and add it
					tableDef.addColumnDefinition(new ColumnDefinition(name, type));
				}
				
				//Apply custom configuration to the target table definition
				tableDef.applyCustomConfiguration(prop);
				
				return tableDef;
			}
		}

		//If it did not return, the format of the definition file is not correct
		FileFormatException formatExcep = new FileFormatException(
				"the format of the definition file is not correct because "
				+ "the line starting with '" 
				+ (tableDef == null ? PREFIX_TABLE_NAME : PREFIX_NUM_COLUMNS) 
				+ "' was not found.");
		LOG.error(formatExcep.getMessage(), formatExcep);
		throw formatExcep;
	}

	public void addColumnDefinition(ColumnDefinition newColumnDefinition) {
		columns_list.add(newColumnDefinition);
		columns_map.put(newColumnDefinition.getName(), newColumnDefinition);
		
		LOG.debug("new column definition: " + newColumnDefinition);
	}

	/**
	 * Apply custom configuration
	 * 
	 * @param prop Configuration properties
	 * @throws FileFormatException 
	 */
	public void applyCustomConfiguration(PropertiesE prop) throws FileFormatException {

		if(prop.containsKey(PropertiesE.IMPALA_TABLE_SCHEMA))
			schemaName = prop.getProperty(PropertiesE.IMPALA_TABLE_SCHEMA);
		
		if(prop.containsKey(PropertiesE.IMPALA_TABLE_NAME))
			tableName = prop.getProperty(PropertiesE.IMPALA_TABLE_NAME);
		
		LOG.debug("the target Impala table name will be: " + schemaName + "." + tableName);
		
		applyCustomColumnConfiguration(prop);		
		
		applyPartitioningColumns(prop);
	}

	private void applyCustomColumnConfiguration(PropertiesE prop) throws FileFormatException {
		HashMap<String, ColumnDefinition> customColumns = prop.getCustomizedColumns();
		
		for (String customColumnName : customColumns.keySet()) {
			Preconditions.checkState(columns_map.containsKey(customColumnName),
					"the column " + customColumnName + " does not exist, "
							+ "you must customize the columns "
							+ "that exist in the definition file.");
			
			ColumnDefinition customColumn = customColumns.get(customColumnName);
			
			// Set custom values
			ColumnDefinition columnDef = columns_map.get(customColumnName);
			columnDef.applyCustom(customColumn);
			
			LOG.debug("applied custom values for column " + customColumnName + ": " + columnDef);
			
			columnsAsSql = null;
		}
	}
	
	private void applyPartitioningColumns(PropertiesE prop) throws FileFormatException {
		LinkedList<PartitioningColumnDefinition> partitioningColumns = prop.getPartitioningColumns();
		
		for (PartitioningColumnDefinition partitioningColumn : partitioningColumns){
			addColumnDefinition(partitioningColumn);
			
			LOG.debug("added partitioning column " + partitioningColumn.getName());
		}
	}
	
	public TableDefinition getDefinitionForStagingTable() {
		TableDefinition stagingTableDef = new TableDefinition(
												schemaName, 
												tableName.concat("_staging"));
		
		for(ColumnDefinition colDef:columns_list){
			if(!(colDef instanceof PartitioningColumnDefinition)){
				stagingTableDef.addColumnDefinition(new ColumnDefinition(colDef.getName(), null, "STRING"));
			}
		}
		
		return stagingTableDef;
	}

	public void log() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("table definition for " + schemaName + "." + tableName + " with columns:");
		
		for(ColumnDefinition colDef:columns_list){
			sb.append("\n  - " + colDef.toString());
		}
		
		LOG.info(sb.toString());
	}
}
