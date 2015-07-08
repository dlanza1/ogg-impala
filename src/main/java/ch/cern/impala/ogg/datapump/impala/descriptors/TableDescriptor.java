package ch.cern.impala.ogg.datapump.impala.descriptors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.impala.ogg.datapump.impala.TypeConverter;
import ch.cern.impala.ogg.datapump.oracle.FileFormatException;
import ch.cern.impala.ogg.datapump.utils.BadConfigurationException;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class TableDescriptor {

	final private static Logger LOG = LoggerFactory.getLogger(TableDescriptor.class);

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

	/**
	 * Name of the table schema
	 */
	String schemaName;
	
	/**
	 * Name of the table
	 */
	String tableName;

	/**
	 * List of columns (useful when we want the columns in order)
	 */
	ArrayList<ColumnDescriptor> columns_list;
	
	/**
	 * List of partition columns
	 */
	ArrayList<ColumnDescriptor> paritioningColumns_list;
	
	/**
	 * Map of columns (useful when we want to find a column by the name)
	 */
	HashMap<String, ColumnDescriptor> columns_map;

	public TableDescriptor(String schema, String table) {
		this.schemaName = schema;
		this.tableName = table;
		
		columns_list = new ArrayList<ColumnDescriptor>();
		paritioningColumns_list = new ArrayList<ColumnDescriptor>();
		columns_map = new HashMap<String, ColumnDescriptor>();
		
		LOG.debug("created table descriptor for schema=" + schema + " table=" + table);
	}

	private TableDescriptor() {
		columns_list = new ArrayList<ColumnDescriptor>();
		paritioningColumns_list = new ArrayList<ColumnDescriptor>();
		columns_map = new HashMap<String, ColumnDescriptor>();
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}
	
	public ArrayList<ColumnDescriptor> getColumnDefinitions() {
		return columns_list;
	}
	
	public ArrayList<ColumnDescriptor> getPartitioningColumnDefinitions() {
		return paritioningColumns_list;
	}

	public static TableDescriptor createFromFile(File definitionFile) throws IOException {

		LOG.info("reading table definition from " + definitionFile.getAbsolutePath());

		TableDescriptor tableDes = null;

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
				
				tableDes = new TableDescriptor(schema, table);
			}else if(line.startsWith(PREFIX_NUM_COLUMNS)){
				
				//If it find the line which contains the number of rows before
				//the line of the table name, the format of the file is not correct
				if(tableDes == null){
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
					tableDes.addColumnDescriptor(new ColumnDescriptor(name, type));
				}
				
				return tableDes;
			}
		}

		//If it did not return, the format of the definition file is not correct
		FileFormatException formatExcep = new FileFormatException(
				"the format of the definition file is not correct because "
				+ "the line starting with '" 
				+ (tableDes == null ? PREFIX_TABLE_NAME : PREFIX_NUM_COLUMNS) 
				+ "' was not found.");
		LOG.error(formatExcep.getMessage(), formatExcep);
		throw formatExcep;
	}

	public void addColumnDescriptor(ColumnDescriptor newColumnDescriptor) {
		if(newColumnDescriptor instanceof PartitioningColumnDescriptor)
			paritioningColumns_list.add(newColumnDescriptor);
		else
			columns_list.add(newColumnDescriptor);
		
		columns_map.put(newColumnDescriptor.getName(), newColumnDescriptor);
		
		LOG.debug("new column descriptor: " + newColumnDescriptor);
	}

	/**
	 * Apply custom configuration
	 * 
	 * @param prop Configuration properties
	 * @throws FileFormatException 
	 * @throws BadConfigurationException 
	 */
	public void applyCustomConfiguration(PropertiesE prop) throws FileFormatException, BadConfigurationException {

		if(prop.containsKey(PropertiesE.IMPALA_TABLE_SCHEMA))
			schemaName = prop.getProperty(PropertiesE.IMPALA_TABLE_SCHEMA);
		
		if(prop.containsKey(PropertiesE.IMPALA_TABLE_NAME))
			tableName = prop.getProperty(PropertiesE.IMPALA_TABLE_NAME);
		
		LOG.debug("the target Impala table name will be: " + schemaName + "." + tableName);
		
		applyCustomColumnConfiguration(prop);		
		
		applyPartitioningColumns(prop);
	}

	private void applyCustomColumnConfiguration(PropertiesE prop) throws FileFormatException {
		HashMap<String, ColumnDescriptor> customColumns = prop.getCustomizedColumns();
		
		for (Map.Entry<String, ColumnDescriptor> entry : customColumns.entrySet()) {
			String customColumnName = entry.getKey();
			ColumnDescriptor customColumn = entry.getValue();
			
			ColumnDescriptor columnDef = columns_map.get(customColumnName);
			if(columnDef != null){
				//If column already exists, customize it
				
				columnDef.applyCustom(customColumn);
				
				LOG.debug("applied custom values for column " + customColumnName + ": " + columnDef);
			}else{
				//If column does not exist, add new column
				
				//Check if all atributtes have been establish
				if(customColumn.getType() == null
						|| customColumn.getExpression() == null)
					throw new FileFormatException("when creating new columns ("
							+ customColumnName + ") data type and expression"
							+ " must be configured");
				
				//Set column name
				customColumn.setName(customColumnName);
				
				//Add it to the collection
				addColumnDescriptor(customColumn);
				
				LOG.debug("added new column " + columnDef);
			}
		}
	}
	
	private void applyPartitioningColumns(PropertiesE prop)
			throws FileFormatException, BadConfigurationException {
		
		LinkedList<PartitioningColumnDescriptor> partitioningColumns = prop.getPartitioningColumns();
		
		for (PartitioningColumnDescriptor partitioningColumn : partitioningColumns){
			addColumnDescriptor(partitioningColumn);
			
			LOG.debug("added partitioning column " + partitioningColumn.getName());
		}
	}
	
	public StagingTableDescriptor getDefinitionForStagingTable() {
		StagingTableDescriptor stagingTableDef = new StagingTableDescriptor(
												schemaName, 
												tableName.concat("_staging"));
		
		for(ColumnDescriptor colDef:columns_list){
			if(!(colDef instanceof PartitioningColumnDescriptor)){
				stagingTableDef.addColumnDescriptor(new ColumnDescriptor(colDef.getName(), "STRING", null));
			}
		}
		
		return stagingTableDef;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("table descriptor (" + schemaName + "." + tableName + ") with columns:");
		
		for(ColumnDescriptor colDef:columns_list){
			sb.append("\n  - " + colDef.toString());
		}
		
		for(ColumnDescriptor colDef:paritioningColumns_list){
			sb.append("\n  - " + colDef.toString());
		}
		
		return sb.toString();
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		TableDescriptor clone = new TableDescriptor();
		
		clone.schemaName = schemaName;
		clone.tableName = tableName;
		
		for (ColumnDescriptor columnDefinition : columns_list) {
			ColumnDescriptor cloneCol = (ColumnDescriptor) columnDefinition.clone();
			
			clone.addColumnDescriptor(cloneCol);
		}
		
		return clone;
	}
}
