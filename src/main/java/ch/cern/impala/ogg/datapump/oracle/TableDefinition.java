package ch.cern.impala.ogg.datapump.oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import ch.cern.impala.ogg.datapump.impala.TypeConverter;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class TableDefinition {

	final private static Logger LOG = LoggerFactory.getLogger(TableDefinition.class);

	private static final String DATATYPE_CONFIG_PREFIX = "impala.table.column.";

	private static final String DATATYPE_CONFIG_SUFFIX = ".datatype";

	private static String PREFIX_TABLE = "Definition for table ";
	private static String PREFIX_NUM_COLUMNS = "Columns: ";

	String schemaName;
	String tableName;

	ArrayList<ColumnDefinition> columns_list;
	HashMap<String, ColumnDefinition> columns_map;
	
	String columnsAsSql = null;
	String columnsAsSqlForExternalTable = null;
	String columnsWithCastingAsSql = null;

	public TableDefinition(String schema, String table) {
		this.schemaName = schema;
		this.tableName = table;
		
		columns_list = new ArrayList<ColumnDefinition>();
		columns_map = new HashMap<String, ColumnDefinition>();
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
	
	public String getColumnsAsSQLForExternalTable() {
		
		if(columnsAsSqlForExternalTable == null){
			for (ColumnDefinition columnDefinition : columns_list) {
				if(columnsAsSqlForExternalTable == null){
					columnsAsSqlForExternalTable = columnDefinition.getName() + " STRING"; 
				}else{
					columnsAsSqlForExternalTable = columnsAsSqlForExternalTable 
									+ ", " + columnDefinition.getName() + " STRING";
				}
			}
		}
		
		return columnsAsSqlForExternalTable;
	}
	
	public String getColumnsWithCastingAsSQL() {
		if(columnsWithCastingAsSql == null){
			for (ColumnDefinition columnDefinition : columns_list) {
				if(columnsWithCastingAsSql == null){
					columnsWithCastingAsSql = columnDefinition.getCastAsSql(); 
				}else{
					columnsWithCastingAsSql = columnsWithCastingAsSql 
									+ ", " + columnDefinition.getCastAsSql();
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

	public static TableDefinition createFromFile(File definitionFile) throws IOException {

		LOG.info("reading table definition from "
				+ definitionFile.getAbsolutePath());

		TableDefinition tableDef = null;

		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(definitionFile));
		String line;
		while ((line = br.readLine()) != null) {
			if(line.startsWith(PREFIX_TABLE)){
				String identifier = line.replaceFirst(PREFIX_TABLE, "");

				String[] fields = identifier.split("\\.");
				
				String schema = fields[0];
				String table = fields[1];
				
				tableDef = new TableDefinition(schema, table);
				
				LOG.info("table definition for schema=" + schema + " table=" + table);
			}else if(line.startsWith(PREFIX_NUM_COLUMNS)){
				String num = line.replaceFirst(PREFIX_NUM_COLUMNS, "");
				
				int numColumns = Integer.valueOf(num);
				
				//Read columns metadata
				for (int c = 0; c < numColumns; c++) {
					line = br.readLine();
					String[] fields = line.replaceAll("\\s+", " ").split(" ");
					
					//Get required columns
					String name = fields[0]; 
					int jdbcType = Integer.valueOf(fields[19]);
					
					//Get Impala column type
					String type = TypeConverter.toImpalaType(jdbcType).toString();
					
					//Create column definition and add it
					tableDef.addColumnDefinition(new ColumnDefinition(c, name, type));
				}
				
				break;
			}
		}

		return tableDef;
	}

	private void addColumnDefinition(ColumnDefinition newColumnDefinition) {
		columns_list.add(newColumnDefinition);
		columns_map.put(newColumnDefinition.getName(), newColumnDefinition);
		
		LOG.info("new column definition: " + newColumnDefinition);
	}

	/**
	 * Apply the changes in the data types
	 * @param prop Configuration properties
	 */
	public void applyCustomConfiguration(PropertiesE prop) {

		if(prop.containsKey(PropertiesE.IMPALA_TABLE_SCHEMA))
			schemaName = prop.getProperty(PropertiesE.IMPALA_TABLE_SCHEMA);
		
		if(prop.containsKey(PropertiesE.IMPALA_TABLE_NAME))
			tableName = prop.getProperty(PropertiesE.IMPALA_TABLE_NAME);
		
		applyCustomDataTypes(prop);		
	}

	private void applyCustomDataTypes(PropertiesE prop) {
		Set<Object> configs = prop.keySet();
		for (Object config_obj : configs) { //Go through every parameter in the configuration
			String config = (String) config_obj;
			
			//If it is a data type configuration
			if(config.startsWith(DATATYPE_CONFIG_PREFIX) && config.endsWith(DATATYPE_CONFIG_SUFFIX)){
				
				String columnName = config.replace(DATATYPE_CONFIG_PREFIX, "")
											.replace(DATATYPE_CONFIG_SUFFIX, "");
				
				String newDataType = prop.getProperty(config);
				
				Preconditions.checkState(columns_map.containsKey(columnName),
						"the column " + columnName + " does not exist, "
								+ "you must configure the data types for columns "
								+ "that exist in the definition file.");
				
				// Set new data type
				ColumnDefinition columnDef = columns_map.get(columnName);
				columnDef.setType(newDataType);
				
				LOG.info("new data type configured for " + columnDef);
				
				columnsAsSql = null;
				columnsAsSqlForExternalTable = null;
			}
		}
	}

}
