package ch.cern.impala.ogg.datapump;

public class Column {

	String name;
	String type;
	
	public Column(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

}
