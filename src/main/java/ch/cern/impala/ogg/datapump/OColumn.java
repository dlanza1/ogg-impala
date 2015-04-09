package ch.cern.impala.ogg.datapump;

public class OColumn {

	String name;
	String type;
	
	public OColumn(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

}
