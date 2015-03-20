package com.oracle.gg.datapump;

import java.math.BigDecimal;
import java.sql.Types;
import java.text.ParseException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;

import com.goldengate.atg.datasource.meta.ColumnMetaData;
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

			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				throw new RuntimeException("the Avro type NULL is not compatibly");
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

			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalBoolean(columnMetaData.getColumnName());
				}else{
					schema.requiredBoolean(columnMetaData.getColumnName());
				}
			}
		},
		/**
		 * (4 bytes) signed integer
		 */
		INT {
			@Override
			public Object getValue(String value) throws ParseException {
				try {
					return Integer.parseInt(value);
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Integer: "
									+ e.getMessage(), 0);
				}
			}
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalInt(columnMetaData.getColumnName());
				}else{
					schema.requiredInt(columnMetaData.getColumnName());
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
					return Long.parseLong(value);
				} catch (Exception e) {
					try{
						String[] split = value.split("\\.");
						
						SettableClock util = new SettableClock();
						util.setDateFormat("yyyy-MM-dd:HH:mm:ss");
						util.setTime(split[0]);
						
						long miliseconds = util.getTime().getTime();
						long nanoseconds = miliseconds * 1000 * 1000;
						
						return nanoseconds + Integer.valueOf(split[1]);
					} catch(Exception ex){
						ex.printStackTrace();
						
						throw new ParseException("there was a problem parsing " + value + " to Long: " + ex.getMessage(), 0);
					}
				}
			}
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalLong(columnMetaData.getColumnName());
				}else{
					schema.requiredLong(columnMetaData.getColumnName());
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
					return Float.parseFloat(value);
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Float: "
									+ e.getMessage(), 0);
				}
			}
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalFloat(columnMetaData.getColumnName());
				}else{
					schema.requiredFloat(columnMetaData.getColumnName());
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
					return Double.parseDouble(value);
				} catch (Exception e) {
					throw new ParseException("there was a problem parsing " + value + " to Double: "
									+ e.getMessage(), 0);
				}
			}
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalDouble(columnMetaData.getColumnName());
				}else{
					schema.requiredDouble(columnMetaData.getColumnName());
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
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalBytes(columnMetaData.getColumnName());
				}else{
					schema.requiredBytes(columnMetaData.getColumnName());
				}
			}
		},
		STRING {
			@Override
			public Object getValue(String value) {
				return value;
			}
			
			@Override
			public void addField(FieldAssembler<Schema> schema,
					ColumnMetaData columnMetaData) {
				if(columnMetaData.isNullable()){
					schema.optionalString(columnMetaData.getColumnName());
				}else{
					schema.requiredString(columnMetaData.getColumnName());
				}
			}
		};

		public abstract Object getValue(String value) throws ParseException;

		public abstract void addField(FieldAssembler<Schema> schema, ColumnMetaData columnMetaData);
	}

	/**
	 * Resolve a database-specific type to the Avro type that should contain it.
	 * 
	 * @throws ParseException 
	 */
	public static AvroType getAvroType(int jdbcType) throws ParseException {

		switch(jdbcType){
		case Types.BIT:
		case Types.BOOLEAN:
			return AvroType.BOOLEAN;
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			return AvroType.INT;
		case Types.BIGINT:
			return AvroType.LONG;
		case Types.REAL:
			return AvroType.FLOAT;
		case 100: //BINARY_FLOAT
		case Types.FLOAT:        
		case Types.DOUBLE:
		case 101: //BINARY_DOUBLE
			return AvroType.DOUBLE;
		case Types.NUMERIC:
		case Types.DECIMAL:
			return AvroType.BYTES;
	    case Types.DATE:
	    case Types.TIME:
	    case Types.TIMESTAMP:
	    	return AvroType.LONG;
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.CLOB:
			return AvroType.STRING;
		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return AvroType.STRING;
		default:
			return AvroType.STRING;
		}
	}
	
}
