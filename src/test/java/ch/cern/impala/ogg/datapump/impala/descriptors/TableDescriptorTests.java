package ch.cern.impala.ogg.datapump.impala.descriptors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import ch.cern.impala.ogg.datapump.impala.TypeConverter.ImpalaType;
import ch.cern.impala.ogg.datapump.utils.BadConfigurationException;
import ch.cern.impala.ogg.datapump.utils.PropertiesE;

public class TableDescriptorTests {

	@Test
	public void createFromFile() throws IOException{
		
		TableDescriptor des = TableDescriptor.createFromFile(new File("src/test/resources/lhclog.def"));
		
		ArrayList<ColumnDescriptor> col_defs = des.getColumnDefinitions();
		
		Assert.assertEquals(3, col_defs.size());
		
		Assert.assertEquals("VARIABLE_ID", col_defs.get(0).getName());
		Assert.assertEquals(ImpalaType.DOUBLE.toString(), col_defs.get(0).getType());
		Assert.assertEquals("cast(VARIABLE_ID as DOUBLE)", col_defs.get(0).getExpression());

		Assert.assertEquals("UTC_STAMP", col_defs.get(1).getName());
		Assert.assertEquals(ImpalaType.TIMESTAMP.toString(), col_defs.get(1).getType());
		Assert.assertEquals("cast(UTC_STAMP as TIMESTAMP)", col_defs.get(1).getExpression());
		
		Assert.assertEquals("VALUE", col_defs.get(2).getName());
		Assert.assertEquals(ImpalaType.DOUBLE.toString(), col_defs.get(2).getType());
		Assert.assertEquals("cast(VALUE as DOUBLE)", col_defs.get(2).getExpression());
	}
	
	@Test
	public void addColumnDescriptor() throws IOException{
		TableDescriptor des = TableDescriptor.createFromFile(new File("src/test/resources/lhclog.def"));
		ArrayList<ColumnDescriptor> col_defs = des.getColumnDefinitions();
		ArrayList<ColumnDescriptor> part_col_defs = des.getPartitioningColumnDefinitions();
		
		des.addColumnDescriptor(new ColumnDescriptor("NAME1", "TYPE1", "EXPR1"));
		
		Assert.assertEquals(4, col_defs.size());
		Assert.assertEquals(0, part_col_defs.size());

		Assert.assertEquals("NAME1", col_defs.get(3).getName());
		Assert.assertEquals("TYPE1", col_defs.get(3).getType());
		Assert.assertEquals("EXPR1", col_defs.get(3).getExpression());
	
		des.addColumnDescriptor(new PartitioningColumnDescriptor("PNAME1", "PTYPE1", "PEXPR1"));
		
		Assert.assertEquals(4, col_defs.size());
		Assert.assertEquals(1, part_col_defs.size());

		Assert.assertEquals("PNAME1", part_col_defs.get(0).getName());
		Assert.assertEquals("PTYPE1", part_col_defs.get(0).getType());
		Assert.assertEquals("PEXPR1", part_col_defs.get(0).getExpression());
	}
	
	@Test
	public void applyCustomConfigurationTableName() throws IOException, BadConfigurationException{
		TableDescriptor des = TableDescriptor.createFromFile(new File("src/test/resources/lhclog.def"));
		
		Assert.assertEquals("LHCLOG", des.getSchemaName());
		Assert.assertEquals("DATA_NUMERIC", des.getTableName());
		
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		des.applyCustomConfiguration(prop);
		
		Assert.assertEquals("LHCLOG", des.getSchemaName());
		Assert.assertEquals("DATA_NUMERIC", des.getTableName());
		
		Mockito.when(prop.getProperty(PropertiesE.IMPALA_TABLE_SCHEMA)).thenReturn("NEW_SCHEMA");
		Mockito.when(prop.containsKey(PropertiesE.IMPALA_TABLE_SCHEMA)).thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.IMPALA_TABLE_NAME)).thenReturn("NEW_NAME");
		Mockito.when(prop.containsKey(PropertiesE.IMPALA_TABLE_NAME)).thenReturn(true);
		
		des.applyCustomConfiguration(prop);
		
		Assert.assertEquals("NEW_SCHEMA", des.getSchemaName());
		Assert.assertEquals("NEW_NAME", des.getTableName());
	}
	
	@Test
	public void applyCustomConfigurationCustomizeColumns() throws IOException, BadConfigurationException{
		TableDescriptor des = TableDescriptor.createFromFile(new File("src/test/resources/lhclog.def"));
		
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		Mockito.when(prop.containsKey(PropertiesE.CUSTOMIZED_COLUMNS_NAMES))
					.thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CUSTOMIZED_COLUMNS_NAMES))
					.thenReturn("UTC_STAMP, VARIABLE_ID,VALUE");
		
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "VALUE" + PropertiesE.NAME_SUFFIX))
					.thenReturn("VALUE_NEW");
		
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "UTC_STAMP" + PropertiesE.NAME_SUFFIX))
					.thenReturn("UTC_STAMP_NEW");
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "UTC_STAMP" + PropertiesE.DATATYPE_SUFFIX))
					.thenReturn("INT");
		
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "VARIABLE_ID" + PropertiesE.NAME_SUFFIX))
					.thenReturn("VARIABLE_ID_NEW");
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "VARIABLE_ID" + PropertiesE.DATATYPE_SUFFIX))
					.thenReturn("STRING");
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "VARIABLE_ID" + PropertiesE.EXPRESSION_SUFFIX))
					.thenReturn("NEW_EXPR");
		
		des.applyCustomConfiguration(prop);
		
		ArrayList<ColumnDescriptor> col_defs = des.getColumnDefinitions();
		ArrayList<ColumnDescriptor> part_col_defs = des.getPartitioningColumnDefinitions();
		
		Assert.assertEquals(3, col_defs.size());
		Assert.assertEquals(0, part_col_defs.size());
		
		Assert.assertEquals("VARIABLE_ID_NEW", col_defs.get(0).getName());
		Assert.assertEquals(ImpalaType.STRING.toString(), col_defs.get(0).getType());
		Assert.assertEquals("NEW_EXPR", col_defs.get(0).getExpression());

		Assert.assertEquals("UTC_STAMP_NEW", col_defs.get(1).getName());
		Assert.assertEquals(ImpalaType.INT.toString(), col_defs.get(1).getType());
		Assert.assertEquals("cast(UTC_STAMP as INT)", col_defs.get(1).getExpression());
		
		Assert.assertEquals("VALUE_NEW", col_defs.get(2).getName());
		Assert.assertEquals(ImpalaType.DOUBLE.toString(), col_defs.get(2).getType());
		Assert.assertEquals("cast(VALUE as DOUBLE)", col_defs.get(2).getExpression());
	}
	
	@Test
	public void applyCustomConfigurationNewColumns() throws IOException, BadConfigurationException{
		TableDescriptor des = TableDescriptor.createFromFile(new File("src/test/resources/lhclog.def"));
		
		PropertiesE prop = new PropertiesE("src/test/resources/empty.properties");
		prop = Mockito.spy(prop);
		
		ArrayList<ColumnDescriptor> col_defs = des.getColumnDefinitions();
		ArrayList<ColumnDescriptor> part_col_defs = des.getPartitioningColumnDefinitions();
		
		Assert.assertEquals(3, col_defs.size());
		Assert.assertEquals(0, part_col_defs.size());
	
		Mockito.when(prop.containsKey(PropertiesE.CUSTOMIZED_COLUMNS_NAMES))
				.thenReturn(true);
		Mockito.when(prop.getProperty(PropertiesE.CUSTOMIZED_COLUMNS_NAMES))
				.thenReturn("C1");
		
		try{
			des.applyCustomConfiguration(prop);
			
			Assert.fail();
		}catch(BadConfigurationException e){
			e.printStackTrace();
		}
		
		
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "C1" + PropertiesE.DATATYPE_SUFFIX))
					.thenReturn("BIGINT");
		try{
			des.applyCustomConfiguration(prop);
			
			Assert.fail();
		}catch(BadConfigurationException e){
			e.printStackTrace();
		}

		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "C1" + PropertiesE.DATATYPE_SUFFIX))
					.thenReturn(null);
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "C1" + PropertiesE.EXPRESSION_SUFFIX))
					.thenReturn("EXPR_C1");
		try{
			des.applyCustomConfiguration(prop);
			
			Assert.fail();
		}catch(BadConfigurationException e){
			e.printStackTrace();
		}
		
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "C1" + PropertiesE.NAME_SUFFIX))
					.thenReturn("C1_NOT_USED_NAME");
		Mockito.when(prop.getProperty(
				PropertiesE.COLUMN_PREFIX + "C1" + PropertiesE.DATATYPE_SUFFIX))
					.thenReturn("BIGINT");
		
		des.applyCustomConfiguration(prop);
		
		Assert.assertEquals(4, col_defs.size());
		Assert.assertEquals(0, part_col_defs.size());
		
		Assert.assertEquals("VARIABLE_ID", col_defs.get(0).getName());
		Assert.assertEquals(ImpalaType.DOUBLE.toString(), col_defs.get(0).getType());
		Assert.assertEquals("cast(VARIABLE_ID as DOUBLE)", col_defs.get(0).getExpression());

		Assert.assertEquals("UTC_STAMP", col_defs.get(1).getName());
		Assert.assertEquals(ImpalaType.TIMESTAMP.toString(), col_defs.get(1).getType());
		Assert.assertEquals("cast(UTC_STAMP as TIMESTAMP)", col_defs.get(1).getExpression());
		
		Assert.assertEquals("VALUE", col_defs.get(2).getName());
		Assert.assertEquals(ImpalaType.DOUBLE.toString(), col_defs.get(2).getType());
		Assert.assertEquals("cast(VALUE as DOUBLE)", col_defs.get(2).getExpression());
		
		Assert.assertEquals("C1", col_defs.get(3).getName());
		Assert.assertEquals(ImpalaType.BIGINT.toString(), col_defs.get(3).getType());
		Assert.assertEquals("EXPR_C1", col_defs.get(3).getExpression());
	}
}
