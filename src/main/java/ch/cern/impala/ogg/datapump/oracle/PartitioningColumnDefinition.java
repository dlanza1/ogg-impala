package ch.cern.impala.ogg.datapump.oracle;

public class PartitioningColumnDefinition extends ColumnDefinition {

	public PartitioningColumnDefinition(String name, String expression, String dataType) {
		super(name, expression, dataType);
	}

	@Override
	public String toString() {
		return "PartitionColumnDefinition [name=" + name + ", type=" + type 
				+ ", expression=" + expression + "]";
	}
}
