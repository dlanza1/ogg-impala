package ch.cern.impala.ogg.datapump.impala.descriptors;


public class PartitioningColumnDescriptor extends ColumnDescriptor {

	public PartitioningColumnDescriptor(String name, String expression, String dataType) {
		super(name, expression, dataType);
	}

	@Override
	public String toString() {
		return "PartitionColumnDescriptor [name=" + name + ", type=" + type 
				+ ", expression=" + expression + "]";
	}
}
