package ch.cern.impala.ogg.datapump.oracle;


public class ColumnDefinition {
	
	int pos;
	
	String name;
	
	String type;

	public ColumnDefinition(int pos, String name, String type) {
		this.pos = pos;
		this.name = name;
		this.type = type;
	}

	public String getCastAsSql(){
		return "cast(" + name + " as " + type + ")";
	}

	@Override
	public String toString() {
		return "ColumnDefinition [pos=" + pos + ", name=" + name
				+ ", type=" + type + "]";
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
	
}
