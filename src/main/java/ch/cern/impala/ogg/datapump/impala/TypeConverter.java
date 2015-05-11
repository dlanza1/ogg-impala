package ch.cern.impala.ogg.datapump.impala;

import java.sql.Types;

public class TypeConverter {
	
	/**
	 * Impala types
	 */
	public static enum ImpalaType {
		BIGINT, //8-byte integer
		BOOLEAN, // single true/false choice.
		CHAR, //A fixed-length character type
		DECIMAL, //A numeric data type with fixed scale and precision
		DOUBLE, //8-byte (double precision) floating-point
		FLOAT, //4-byte (single precision) floating-point
		INT, //4-byte integer
		REAL, //An alias for the DOUBLE data type
		SMALLINT, //2-byte integer
		STRING, 
		TIMESTAMP, // Represents a point in time.
		TINYINT // 1-byte integer
	}

	/**
	 * Resolve a jdbc type to the corresponding Impala type
	 * 
	 * @throws ParseException 
	 */
	public static ImpalaType toImpalaType(int jdbcType){
		switch(jdbcType){
		case Types.BIT:
		case Types.BOOLEAN:
			return ImpalaType.BOOLEAN;
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			return ImpalaType.INT;
		case Types.BIGINT:
			return ImpalaType.BIGINT;
		case Types.REAL:
		case 100: //BINARY_FLOAT
			return ImpalaType.FLOAT;
		case Types.FLOAT:        
			return ImpalaType.DOUBLE;
		case Types.DOUBLE:
		case 101: //BINARY_DOUBLE
		case Types.NUMERIC:
		case Types.DECIMAL:
			return ImpalaType.DOUBLE;
	    case Types.DATE:
	    case Types.TIME:
	    case Types.TIMESTAMP:
	    case 187: //TIMESTAMP
	    	return ImpalaType.TIMESTAMP;
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.CLOB:
			return ImpalaType.STRING;
		case Types.BLOB:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return ImpalaType.STRING;
		default:
			return ImpalaType.STRING;
		}
	}

}
