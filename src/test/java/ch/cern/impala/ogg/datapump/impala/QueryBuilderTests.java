package ch.cern.impala.ogg.datapump.impala;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.cern.impala.ogg.datapump.impala.descriptors.ColumnDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.PartitioningColumnDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.StagingTableDescriptor;
import ch.cern.impala.ogg.datapump.impala.descriptors.TableDescriptor;


public class QueryBuilderTests {
	
	static QueryBuilder qb;
	
	@BeforeClass
	public static void setup() {
		qb = new QueryBuilder(null);
	}

	@Test
	public void createExternalTableQuery(){
		TableDescriptor des = new TableDescriptor("schema", "table");
		des.addColumnDescriptor(new ColumnDescriptor("c1", "BIGINT", "e1"));
		des.addColumnDescriptor(new ColumnDescriptor("c2", "INT", "e2"));
		
		des = des.getDefinitionForStagingTable();
		
		Path path = new Path("hdfs://localhost:1234/path/to/external/table/directory");
		
		Query q = qb.createExternalTable(des, path);
		Assert.assertEquals("CREATE EXTERNAL TABLE schema.table_staging "
				+ "(c1 STRING, c2 STRING) STORED "
				+ "AS textfile "
				+ "LOCATION '/path/to/external/table/directory'", 
				q.getStatement());	
		
		// External table (staging/temporal) should not contain partitioning columns
		des.addColumnDescriptor(new PartitioningColumnDescriptor(
				"p1", "DECIMAL(5,1)", "cast(c2 as DECIMAL(5,1))"));
		q = qb.createExternalTable(des, path);
		Assert.assertEquals("CREATE EXTERNAL TABLE schema.table_staging "
				+ "(c1 STRING, c2 STRING) "
				+ "STORED AS textfile "
				+ "LOCATION '/path/to/external/table/directory'", 
				q.getStatement());	
	}
	
	@Test
	public void createFinalTableQuery(){
		TableDescriptor des = new TableDescriptor("schema", "table");
		des.addColumnDescriptor(new ColumnDescriptor("c1", "BIGINT"));
		des.addColumnDescriptor(new ColumnDescriptor("c2", "BOOLEAN"));
		
		Query q = qb.createTable(des);
		Assert.assertEquals("CREATE TABLE schema.table "
				+ "(c1 BIGINT, c2 BOOLEAN) STORED "
				+ "AS parquet", 
				q.getStatement());	
		
		des.addColumnDescriptor(new PartitioningColumnDescriptor(
				"p1", "DECIMAL(5,1)", "cast(c1 as DECIMAL(5,1))"));
		des.addColumnDescriptor(new PartitioningColumnDescriptor(
				"p2", "BIGINT", "cast(c2 as BIGINT)"));
		q = qb.createTable(des);
		Assert.assertEquals("CREATE TABLE schema.table "
				+ "(c1 BIGINT, c2 BOOLEAN) "
				+ "PARTITIONED BY (p1 DECIMAL(5,1), p2 BIGINT) "
				+ "STORED AS parquet", 
				q.getStatement());	
	}

	@Test
	public void insertIntoQuery(){
		TableDescriptor targetDes = new TableDescriptor("schema", "table");
		targetDes.addColumnDescriptor(new ColumnDescriptor("c1", "INT"));
		targetDes.addColumnDescriptor(new ColumnDescriptor("c2", "BOOLEAN"));
		
		StagingTableDescriptor sourceDes = targetDes.getDefinitionForStagingTable();
		
		Query q = qb.insertInto(sourceDes, targetDes, -1);
		Assert.assertEquals("INSERT INTO schema.table "
				+ "SELECT cast(c1 as INT), "
				+ "cast(c2 as BOOLEAN) "
				+ "FROM schema.table_staging", 
				q.getStatement());
		
		targetDes.addColumnDescriptor(new PartitioningColumnDescriptor(
				"p1", "DECIMAL(5,1)", "cast(c1 as DECIMAL(5,1))"));
		targetDes.addColumnDescriptor(new PartitioningColumnDescriptor(
				"p2", "BIGINT", "cast(c2 as BIGINT)"));
		
		q = qb.insertInto(sourceDes, targetDes, -1);
		Assert.assertEquals("INSERT INTO schema.table "
				+ "PARTITION (p1, p2) "
				+ "SELECT cast(c1 as INT), "
				+ "cast(c2 as BOOLEAN), "
				+ "cast(c1 as DECIMAL(5,1)), "
				+ "cast(c2 as BIGINT) "
				+ "FROM schema.table_staging", 
				q.getStatement());
		
		q = qb.insertInto(sourceDes, targetDes, 1024);
		Assert.assertEquals("set PARQUET_FILE_SIZE=1024; "
				+ "INSERT INTO schema.table "
				+ "PARTITION (p1, p2) "
				+ "SELECT cast(c1 as INT), "
				+ "cast(c2 as BOOLEAN), "
				+ "cast(c1 as DECIMAL(5,1)), "
				+ "cast(c2 as BIGINT) "
				+ "FROM schema.table_staging", 
				q.getStatement());
	}
	
	@Test
	public void dropTableQuery(){
		TableDescriptor des = new TableDescriptor("schema", "table");
		
		Query q = qb.dropTable(des);
		
		Assert.assertEquals("DROP TABLE schema.table", q.getStatement());
	}
	
}
