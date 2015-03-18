package com.oracle.gg.datapump;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;

import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.meta.DsType;
import com.goldengate.atg.util.SettableClock;

public class TypeConverter {
	
	/**
	 * Primitive Avro tpyes
	 */
	public static enum AvroType {
		/**
		 * no value
		 */
		NULL {
			@Override
			public Object getValue(String value) {
				return null;
			}
		},
		/**
		 * a binary value
		 */
		BOOLEAN { 
			@Override
			public Object getValue(String value) throws ParseException {
				if(value.toLowerCase().equals("true"))
					return true;
				else if(value.toLowerCase().equals("false"))
					return false;
				else
					throw new ParseException("there was a problem parsing " + value + " to Boolean: "
									+ " no contain 'true' or 'false' (no case sensitive)", 0);
			}
		},
		/**
		 * (4 bytes) signed integer
		 */
		INT {
			@Override
			public Object getValue(String value) throws ParseException {
				try {
					return Integer.parseInt(removeUndesirableCharacters(value));
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Integer: "
									+ e.getMessage(), 0);
				}
			}
		},
		/**
		 * (8 bytes) signed integer
		 */
		LONG {
			@Override
			public Object getValue(String value) throws ParseException {
				try {
					return Long.parseLong(removeUndesirableCharacters(value));
				} catch (Exception e) {
					try{
						SettableClock util = new SettableClock(SettableClock.defaultParseDate(value));
//						util.setDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
//						util.setTime(value);
						
						return util.getTime().getTime();
					} catch(Exception ex){
						ex.printStackTrace();
						
						throw new ParseException("there was a problem parsing " + value + " to Long: " + ex.getMessage(), 0);
					}
				}
			}
		},
		/**
		 * single precision (4 bytes) IEEE 754 floating-point number
		 */
		FLOAT {
			@Override
			public Object getValue(String value) throws ParseException {
				try {
					return Float.parseFloat(removeUndesirableCharacters(value));
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Float: "
									+ e.getMessage(), 0);
				}
			}
		},
		/**
		 * double precision (8 bytes) IEEE 754 floating-point number
		 */
		DOUBLE {
			@Override
			public Object getValue(String value) throws ParseException {
				try {
					return Double.parseDouble(removeUndesirableCharacters(value));
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Double: "
									+ e.getMessage(), 0);
				}
			}
		},
		/**
		 * sequence of 8-bit unsigned bytes
		 */
		BYTES {
			@Override
			public Object getValue(String value) {
				BigDecimal decimal = new BigDecimal(value);

				return decimal.unscaledValue().toByteArray();
			}
		},
		STRING {
			@Override
			public Object getValue(String value) {
				return value;
			}
		};

		public abstract Object getValue(String value) throws ParseException;

	}

	/**
	 * Resolve a database-specific type to the Java type that should contain it.
	 * 
	 * @throws ParseException 
	 */
	public static Object toAvro(Col col) throws ParseException {
		DsType type = col.getDataType();
		
		if(col.isValueNull())
			return null;
		
		if(col.isMissing())
			throw new ParseException("the value is missing, it should be stored", 0);
		
		switch(col.getDataType().getJDBCType()){
		case Types.BIT:
		case Types.BOOLEAN:
			return AvroType.BOOLEAN.getValue(col.getValue());
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			return AvroType.INT.getValue(col.getValue());
		case Types.BIGINT:
			return AvroType.LONG.getValue(col.getValue());
		case Types.REAL:
		case 100: //BINARY_FLOAT
			return AvroType.FLOAT.getValue(col.getValue());
		case Types.FLOAT:        
			if(col.getDataType().getPrecision() <= 23){
				return AvroType.FLOAT.getValue(col.getValue());
			}else{
				return AvroType.DOUBLE.getValue(col.getValue());
			}
		case Types.DOUBLE:
		case 101: //BINARY_DOUBLE
			return AvroType.DOUBLE.getValue(col.getValue());
		case Types.NUMERIC:
		case Types.DECIMAL:
			return AvroType.BYTES.getValue(col.getValue());
	    case Types.DATE:
	    case Types.TIME:
	    case Types.TIMESTAMP:
	    	return AvroType.LONG.getValue(col.getValue());
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.CLOB:
			return AvroType.STRING.getValue(col.getValue());
		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return AvroType.STRING.getValue(col.getValue());
		default:
			throw new ParseException("the JDBCT type " + col.getDataType().getJDBCType() + " is not compatible", 0);
		}
	}
	
	/**
	 * Resolve a database-specific type to the Java type that should contain it.
	 * 
	 * @throws ParseException 
	 * @throws SQLException 
	 */
	public static Object toOracleSQLType(Col col) throws ParseException, SQLException {		
		if(col.isValueNull())
			return null;
		
		if(col.isMissing())
			throw new ParseException("the value is missing, it should be stored", 0);
		
		//http://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm
		
		switch(col.getDataType().getJDBCType()){
		case Types.BIT:
		case Types.BOOLEAN:
			return oracle.sql.NUMBER.toBoolean(col.getBinary());
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			return oracle.sql.NUMBER.toInt(col.getBinary());
		case Types.BIGINT:
			return oracle.sql.NUMBER.toLong(col.getBinary());
		case Types.REAL:
		case 100: //BINARY_FLOAT
			return oracle.sql.NUMBER.toFloat(col.getBinary());
		case Types.FLOAT:        
			return oracle.sql.NUMBER.toDouble(col.getBinary());
		case Types.DOUBLE:
		case 101: //BINARY_DOUBLE
			return oracle.sql.NUMBER.toDouble(col.getBinary());
		case Types.NUMERIC:
		case Types.DECIMAL:
			return oracle.sql.NUMBER.toBigDecimal(col.getBinary());
	    case Types.DATE:
	    	return oracle.sql.DATE.toDate(col.getBinary());
	    case Types.TIME:
	    case Types.TIMESTAMP:
	    	return oracle.sql.TIMESTAMP.toTimestamp(col.getBinary());
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.CLOB:
			return new String(col.getBinary());
		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return new String(col.getBinary());
		default:
			throw new ParseException("the JDBCT type " + col.getDataType().getJDBCType() + " is not compatible", 0);
		}
	}

	public static String removeUndesirableCharacters(String value) {
		return value;
//		return value.replaceAll("[\\D&&[^\\.]]", "")
//				.replaceAll("[\\D]", "");
	}
}
