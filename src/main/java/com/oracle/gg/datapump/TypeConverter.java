package com.oracle.gg.datapump;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;

import oracle.jdbc.OracleTypes;

import org.apache.log4j.Logger;

import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.util.SettableClock;

public class TypeConverter {
	
	final private static Logger LOG = Logger.getLogger(TypeConverter.class);
	
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
						SettableClock util = new SettableClock();
						util.setDateFormat("yyyy-MM-dd:HH:mm:ss.SSSSSSSSS");
						util.setTime(value);
						
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
		if(col.getAfter().isValueNull())
			return null;
		
		if(col.isMissing())
			throw new ParseException("the value is missing, it should be stored", 0);
		
		String val = col.getAfter().getValue();
		
		switch(col.getDataType().getJDBCType()){
		case Types.BIT:
		case Types.BOOLEAN:
			return AvroType.BOOLEAN.getValue(val);
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			return AvroType.INT.getValue(val);
		case Types.BIGINT:
			return AvroType.LONG.getValue(val);
		case Types.REAL:
		case 100: //BINARY_FLOAT
			return AvroType.FLOAT.getValue(val);
		case Types.FLOAT:        
			if(col.getDataType().getPrecision() <= 23){
				return AvroType.FLOAT.getValue(val);
			}else{
				return AvroType.DOUBLE.getValue(val);
			}
		case Types.DOUBLE:
		case 101: //BINARY_DOUBLE
			return AvroType.DOUBLE.getValue(val);
		case Types.NUMERIC:
		case Types.DECIMAL:
			return AvroType.BYTES.getValue(val);
	    case Types.DATE:
	    case Types.TIME:
	    case Types.TIMESTAMP:
	    	return AvroType.LONG.getValue(val);
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.CLOB:
			return AvroType.STRING.getValue(val);
		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return AvroType.STRING.getValue(val);
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
		LOG.warn(col);
		LOG.warn(col.getAfter());
		LOG.warn(col.getDataType());
		LOG.warn(col.getDataType().getJDBCType());
		
		if(col.getAfter().isValueNull())
			return null;
		
		if(col.isMissing())
			throw new ParseException("the value is missing, it should be stored", 0);
		
		byte[] val = col.getAfter().getBinary();
		
		LOG.warn(val);
		
		//http://docs.oracle.com/cd/E11882_01/java.112/e16548/datacc.htm#JJDBC28365
		switch(col.getDataType().getJDBCType()){
		case OracleTypes.CHAR:
		case OracleTypes.VARCHAR:
		case OracleTypes.LONGVARCHAR:
			return new String(val);
		case OracleTypes.NUMERIC:
		case OracleTypes.DECIMAL:
			return oracle.sql.NUMBER.toBigDecimal(val).unscaledValue().toByteArray();
		case OracleTypes.BIT:
			return oracle.sql.NUMBER.toBoolean(val);
		case OracleTypes.TINYINT:
			return oracle.sql.NUMBER.toByte(val);
		case OracleTypes.SMALLINT:
			return oracle.sql.NUMBER.toShort(val);
		case OracleTypes.INTEGER:
			return oracle.sql.NUMBER.toInt(val);
		case OracleTypes.BIGINT:
			return oracle.sql.NUMBER.toLong(val);
		case OracleTypes.REAL:
			return oracle.sql.NUMBER.toFloat(val);
		case OracleTypes.FLOAT:
		case OracleTypes.BINARY_FLOAT:
		case OracleTypes.DOUBLE:
		case OracleTypes.BINARY_DOUBLE:
			return oracle.sql.NUMBER.toDouble(val);
		case OracleTypes.DATE:
		case OracleTypes.TIME:
			return oracle.sql.DATE.toDate(val).getTime() * 1000 * 1000;
		case OracleTypes.TIMESTAMP:
		case OracleTypes.TIMESTAMPTZ:
			return oracle.sql.TIMESTAMP.toTimestamp(val).getTime() * 1000 * 1000;
		default:
			return col.getAfter().getValue();
		}
		
	}
 
	public static String removeUndesirableCharacters(String value) {
		return value;
//		return value.replaceAll("[\\D&&[^\\.]]", "")
//				.replaceAll("[\\D]", "");
	}
}
