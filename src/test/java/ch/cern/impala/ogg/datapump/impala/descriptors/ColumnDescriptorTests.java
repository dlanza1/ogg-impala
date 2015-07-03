package ch.cern.impala.ogg.datapump.impala.descriptors;


import org.junit.Assert;
import org.junit.Test;

public class ColumnDescriptorTests {
	
	@Test
	public void expressionGeneration(){
		ColumnDescriptor c = new ColumnDescriptor("COL_1", "INT");
		
		Assert.assertEquals("cast(COL_1 as INT)", c.expression);
	}
	
	@Test
	public void customizationWithExpression(){
		ColumnDescriptor ori = new ColumnDescriptor("COL_1", "INT");
		
		ColumnDescriptor cust = new ColumnDescriptor("COL_2", "STRING", "expr1");
		
		ori.applyCustom(cust);
		
		Assert.assertEquals("COL_2", ori.name);
		Assert.assertEquals("STRING", ori.type);
		Assert.assertEquals("expr1", ori.expression);
	}
	
	@Test
	public void customizationWithoutExpression(){
		ColumnDescriptor ori = new ColumnDescriptor("COL_1", "INT");
		
		ColumnDescriptor cust = new ColumnDescriptor("COL_2", "STRING", null);
		
		ori.applyCustom(cust);
		
		Assert.assertEquals("COL_2", ori.name);
		Assert.assertEquals("STRING", ori.type);
		Assert.assertEquals("cast(COL_1 as STRING)", ori.expression);
	}
	
	@Test
	public void customizationWithoutExpressionAndWithoutType(){
		ColumnDescriptor ori = new ColumnDescriptor("COL_1", "INT");
		
		ColumnDescriptor cust = new ColumnDescriptor("COL_2", null, null);
		
		ori.applyCustom(cust);
		
		Assert.assertEquals("COL_2", ori.name);
		Assert.assertEquals("INT", ori.type);
		Assert.assertEquals("cast(COL_1 as INT)", ori.expression);
	}
	
	@Test
	public void customizationWithoutExpressionAndWithType(){
		ColumnDescriptor ori = new ColumnDescriptor("COL_1", "INT");
		
		ColumnDescriptor cust = new ColumnDescriptor(null, "BIGINT", null);
		
		ori.applyCustom(cust);
		
		Assert.assertEquals("COL_1", ori.name);
		Assert.assertEquals("BIGINT", ori.type);
		Assert.assertEquals("cast(COL_1 as BIGINT)", ori.expression);
	}
}
