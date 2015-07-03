package ch.cern.impala.ogg.datapump.impala.descriptors;

public class ColumnDescriptor {
	
	protected String name;
	
	protected String type;
	
	/**
	 * Impala format expression which will be used to
	 * calculate the value of the column.
	 */
	protected String expression;
	
	/**
	 * Set name, type and expression with argument values.
	 * 
	 * @param name
	 * @param type
	 * @param expression
	 */
	public ColumnDescriptor(String name, String type, String expression) {
		this.name = name;
		this.type = type;
		this.expression = expression;
	}

	/**
	 * Set name and type with argument values.
	 * 
	 * Set expression with a casting which use the name 
	 * and the type.
	 * 
	 * @param name
	 * @param type
	 */
	public ColumnDescriptor(String name, String type) {
		this.name = name;
		this.type = type;
		
		this.expression = "cast(" + name + " as " + type + ")";
	}
	
	public ColumnDescriptor() {
	}

	public String getExpression(){
		return expression;
	}

	@Override
	public String toString() {
		return "ColumnDescriptor [name=" + name + ", type=" + type
				+ ", expression=" + expression + "]";
	}

	public String getName() {
		return name;
	}

	public void setType(String newDataType) {	
		this.type = newDataType;
	}

	public String getType() {
		return type;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Apply a custom definition 
	 * 
	 * Column descriptor send as argument should have
	 * established the attribute which must be updated,
	 * otherwise they must be null. Exception with
	 * expression attribute, it follows a special logic:
	 *  - If new expression is specified (!= null), it is set.
	 *  - If no new expression is specified (null) and new data type 
	 *  is specified (!= null), expression is updated with
	 *  a casting which use old column name and new data type.
	 * 
	 * @param custom ColumnDescriptor with new values
	 */
	public void applyCustom(ColumnDescriptor custom) {
		
		if(custom.getExpression() != null){
			//If new expression is specified, set new value
			setExpression(custom.getExpression());
		}else if(custom.getType() != null){
			//If no new expression is specified and
			//new data type is specified, update expression
			//with old column name and new data type
			setExpression("cast(" + name + " as " + custom.getType() + ")");
		}
		
		if(custom.getName() != null)
			setName(custom.getName());
		
		if(custom.getType() != null)
			setType(custom.getType());
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		ColumnDescriptor clone = new ColumnDescriptor();
		
		clone.name = name;
		clone.type = type;
		clone.expression = expression;
		
		return clone;
	}
}
