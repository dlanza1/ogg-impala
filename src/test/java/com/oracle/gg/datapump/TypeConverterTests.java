package com.oracle.gg.datapump;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.meta.DsType;
import com.oracle.gg.datapump.TypeConverter.AvroType;

public class TypeConverterTests {
	
	@Test
	public void integer() {
		//http://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj10696.html#rrefsqlj10696
		String oracle_max_integer = "2147483647";
		String oracle_min_integer = "-2147483648";
		
		Col col = getMockedCol(oracle_max_integer, Types.INTEGER);
		
		try {
			AvroType avroType = TypeConverter.getAvroType(col.getDataType().getJDBCType());
			Object value = avroType.getValue(col.getValue());
			
			Assert.assertEquals(oracle_max_integer, value.toString());
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		col = getMockedCol(oracle_min_integer, Types.INTEGER);
		
		try {
			AvroType avroType = TypeConverter.getAvroType(col.getDataType().getJDBCType());
			Object value = avroType.getValue(col.getValue());
			
			Assert.assertEquals(oracle_min_integer, value.toString());
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
//		System.out.println(Float.MAX_VALUE);
	}
	
	@Test
	public void decimal() throws ParseException {
		
		String decimal = "891275612501236589.019";
		int scale = new BigDecimal(decimal).scale();

		Col col = getMockedCol(decimal, Types.DECIMAL);
		
		AvroType avroType = TypeConverter.getAvroType(col.getDataType().getJDBCType());
		byte[] value = (byte[]) avroType.getValue(col.getValue());
		
		Assert.assertEquals(decimal, new BigDecimal(new BigInteger(value), scale).toString());
	}

	@Test
	public void timestamp() throws ParseException {
		String date = "2015-03-19:17:14:02";
		String millis = "123456789";

		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
		cal.setTime(sdf.parse(date));
		
		Col col = getMockedCol(date + "." + millis, Types.TIMESTAMP);
		
		AvroType avroType = TypeConverter.getAvroType(col.getDataType().getJDBCType());
		long value = (long) avroType.getValue(col.getValue());
		
		//Check seconds
		Calendar cal_avro = Calendar.getInstance();
		cal_avro.setTimeInMillis(value / (1000 * 1000 * 1000) * 1000);
		Assert.assertEquals(cal.getTime().getTime(), cal_avro.getTime().getTime());
		
		//Check milliseconds
		Assert.assertEquals(Long.parseLong(millis), value % (1000 * 1000 * 1000));
	}

	private Col getMockedCol(String return_value, int jdbcType) {

		DsColumn dsColumn = mock(DsColumn.class);
		doReturn(return_value).when(dsColumn).getValue();
		doReturn(false).when(dsColumn).isValueNull();
		Col col = mock(Col.class);
		doReturn(return_value).when(col).getValue();
		doReturn(dsColumn).when(col).getAfter();
		DsType dsType = mock(DsType.class);
		doReturn(jdbcType).when(dsType).getJDBCType();
		doReturn(dsType).when(col).getDataType();
		
		return col;
	}
}
