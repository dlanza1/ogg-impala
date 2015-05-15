package ch.cern.impala.ogg.datapump.oracle;



public class ColumnDefinition {
	
	protected String name;
	
	protected String originalName;
	
	protected String type;
	
	/**
	 * Impala format expression which will be used to
	 * calculate the value of the column
	 */
	protected String expression;
	
	public ColumnDefinition(String name, String expression, String type) {
		this.name = name;
		this.originalName = name;
		this.type = type;
		this.expression = expression;
	}

	public ColumnDefinition(String name, String type) {
		this.name = name;
		this.originalName = name;
		this.type = type;
		
		this.expression = "cast(" + name + " as " + type + ")";
	}
	
	public String getExpression(){
		return expression;
	}

	@Override
	public String toString() {
		return "ColumnDefinition [name=" + name + ", type=" + type
				+ ", expression=" + expression + "]";
	}

	public String getName() {
		return name;
	}

	public void setType(String newDataType) {
		this.type = newDataType;

		this.expression = "cast(" + originalName + " as " + newDataType + ")";
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
	public void applyCustom(ColumnDefinition custom) {
		
		if(custom.getName() != null)
			setName(custom.getName());
		
		if(custom.getType() != null)
			setType(custom.getType());
		
		if(custom.getExpression() != null)
			setExpression(custom.getExpression());
	}
}
