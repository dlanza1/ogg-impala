package ch.cern.impala.ogg.datapump;

public class ITable {

	private ImpalaClient impalaClient;
	private OTableMetadata metadata;

	public ITable(ImpalaClient impalaClient, OTableMetadata metadata) {
		this.impalaClient = impalaClient;
		this.metadata = metadata;
	}

	public boolean exist() {
		// TODO Auto-generated method stub
		return false;
	}

	public void insertoInto(ITable finalTable) {
		
	}

	public void create() {
		// TODO Auto-generated method stub
		
	}

	public void drop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		return metadata.getSchemaName() + "." + metadata.getTableName();
	}
}
