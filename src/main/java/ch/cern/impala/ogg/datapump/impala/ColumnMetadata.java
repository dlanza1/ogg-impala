package ch.cern.impala.ogg.datapump.impala;

public class ColumnMetadata {

	String name;
	String type;
	
	public ColumnMetadata(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

}
