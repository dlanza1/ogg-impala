package ch.cern.impala.ogg.datapump.impala.descriptors;

public class ColumnDescriptor {
	
	protected String name;
	
	protected String type;
	
	/**
	 * Impala format expression which will be used to
	 * calculate the value of the column
	 */
	protected String expression;
	
	public ColumnDescriptor(String name, String expression, String type) {
		this.name = name;
		this.type = type;
		this.expression = expression;
		
		//TODO change arguments order
		
		//TODO when type is null, generate expression
	}

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
		
		//Setting type can not generate new cast expression because
		//name could have been changed
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
	 * @param custom
	 */
	public void applyCustom(ColumnDescriptor custom) {
		
		if(custom.getName() != null)
			setName(custom.getName());
		
		if(custom.getType() != null)
			setType(custom.getType());
		
		if(custom.getExpression() != null)
			setExpression(custom.getExpression());
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
