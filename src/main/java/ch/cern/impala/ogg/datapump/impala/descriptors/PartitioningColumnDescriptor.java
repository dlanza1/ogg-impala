package ch.cern.impala.ogg.datapump.impala.descriptors;


public class PartitioningColumnDescriptor extends ColumnDescriptor {

	public PartitioningColumnDescriptor(String name, String dataType, String expression) {
		super(name, dataType, expression);
	}

	@Override
	public String toString() {
		return "PartitionColumnDescriptor [name=" + name + ", type=" + type 
				+ ", expression=" + expression + "]";
	}
}
