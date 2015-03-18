package com.oracle.gg.datapump;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.meta.DsType;

public class TypeConverterTests {
	
	@Test
	public void integer() {
		//http://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj10696.html#rrefsqlj10696
		String oracle_max_integer = "2147483647";
		String oracle_min_integer = "-2147483648";
		
		Col col = mock(Col.class);
		doReturn(oracle_max_integer).when(col).getValue();
		DsType dsType = mock(DsType.class);
		doReturn(Types.INTEGER).when(dsType).getJDBCType();
		doReturn(dsType).when(col).getDataType();
		
		try {
			Assert.assertEquals(oracle_max_integer, TypeConverter.toAvro(col).toString());
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		doReturn(oracle_min_integer).when(col).getValue();
		try {
			Assert.assertEquals(oracle_min_integer, TypeConverter.toAvro(col).toString());
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

		Col col = mock(Col.class);
		doReturn(decimal).when(col).getValue();
		DsType dsType = mock(DsType.class);
		doReturn(Types.DECIMAL).when(dsType).getJDBCType();
		doReturn(dsType).when(col).getDataType();
		
		byte[] avro = (byte[]) TypeConverter.toAvro(col);
		
		Assert.assertEquals(decimal, new BigDecimal(new BigInteger(avro), scale).toString());
	}

	@Test
	public void timestamp() throws ParseException {
		String time = "2009-01-15 12:54:16.123";
		
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.FRANCE);
		cal.setTime(sdf.parse(time));
		
		Col col = mock(Col.class);
		doReturn(time).when(col).getValue();
		DsType dsType = mock(DsType.class);
		doReturn(Types.TIMESTAMP).when(dsType).getJDBCType();
		doReturn(dsType).when(col).getDataType();
		
		Long avro = (Long) TypeConverter.toAvro(col);
		
		Calendar cal_avro = Calendar.getInstance();
		cal_avro.setTimeInMillis(avro);
		
		Assert.assertEquals(time, sdf.format(cal_avro.getTime()));
	
	}
}
