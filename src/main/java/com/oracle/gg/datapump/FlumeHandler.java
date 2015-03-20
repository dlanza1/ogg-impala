package com.oracle.gg.datapump;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import oracle.jdbc.OracleTypes;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetDescriptor.Builder;
import org.kitesdk.data.Datasets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsOperation.OpType;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class FlumeHandler extends AbstractHandler {
	final private static Logger LOG = LoggerFactory.getLogger(FlumeHandler.class);
	
	private URI dataset_uri;
	private GenericRecordBuilder recordBuilder;
	private Schema schema;

	private FlumeClient flumeClient;

	private String sourceTable;
	
	public FlumeHandler() {
		flumeClient = getFlumeClient();
	}
	
	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		LOG.info("Initializing handler...");
		
		if(dataset_uri == null)
			throw new RuntimeException("the dataset must be specified in the properties file");
		
		informInit(conf, metaData); 
		
		flumeClient.connect();
		
		Dataset<Record> dataset;
		if(Datasets.exists(dataset_uri)){
			LOG.info("loading dataset: " + dataset_uri);
			dataset = (Dataset<Record>) Datasets.load(dataset_uri, Record.class);
		}else{
			LOG.info("the dataset "+dataset_uri+" does not exist, so it is going to be created");
			DatasetDescriptor descriptor = getDescriptor(metaData);
			dataset = (Dataset<Record>) Datasets.create(dataset_uri, descriptor, Record.class);
		}
		
		schema = dataset.getDescriptor().getSchema();
		LOG.info("Dataset schema = " + schema);
		
		recordBuilder = new GenericRecordBuilder(schema);
		
		LOG.info("Handler was inicialized");
	}
	
	private DatasetDescriptor getDescriptor(DsMetaData metaData) {
		TableMetaData tableMetadata = metaData.getTableMetaData(new TableName(sourceTable));
		
		ArrayList<ColumnMetaData> columnsMetadata = tableMetadata.getColumnMetaData();
		
		FieldAssembler<Schema> schema = SchemaBuilder.record("record").fields();

		for (ColumnMetaData columnMetaData : columnsMetadata) {
			addField(schema, columnMetaData);
		}
		
		Builder builder = new DatasetDescriptor.Builder();
		builder.schema(schema.endRecord());
		builder.format("parquet");
		
		return builder.build();
	}

	private void addField(FieldAssembler<Schema> schema, ColumnMetaData columnMetaData) {
		String columnName = columnMetaData.getColumnName();
		
		switch(columnMetaData.getDataType().getJDBCType()){
		case OracleTypes.CHAR:
		case OracleTypes.VARCHAR:
		case OracleTypes.LONGVARCHAR:
			schema.requiredString(columnName);
			return;
		case OracleTypes.NUMERIC:
		case OracleTypes.DECIMAL:
			schema.requiredBytes(columnName);
			return;
		case OracleTypes.BIT:
			schema.requiredBoolean(columnName);
		case OracleTypes.TINYINT:
		case OracleTypes.SMALLINT:
		case OracleTypes.INTEGER:
			schema.requiredInt(columnName);
			return;
		case OracleTypes.BIGINT:
			schema.requiredLong(columnName);
			return;
		case OracleTypes.REAL:
			schema.requiredFloat(columnName);
			return;
		case OracleTypes.FLOAT:
		case OracleTypes.BINARY_FLOAT:
		case OracleTypes.DOUBLE:
		case OracleTypes.BINARY_DOUBLE:
			schema.requiredDouble(columnName);
			return;
		case OracleTypes.DATE:
		case OracleTypes.TIME:
		case OracleTypes.TIMESTAMP:
		case OracleTypes.TIMESTAMPTZ:
			schema.requiredLong(columnName);
			return;
		default:
			schema.requiredString(columnName);
			return;
		}
	}

	protected void informInit(DsConfiguration conf, DsMetaData metaData) {
		super.init(conf, metaData);
	}
	
	@Override
	public Status transactionBegin(DsEvent e, DsTransaction tx) {
		informTransactionBegin(e, tx);
		
		return Status.OK;
	}
	
	protected void informTransactionBegin(DsEvent e, DsTransaction tx){
		super.transactionBegin(e, tx);
	}
	
	@Override
	public Status operationAdded(DsEvent event, DsTransaction transaction, DsOperation operation) {
		Status retVal = informOperationAdded(event, transaction, operation);
		
		try{
			if(isOperationMode()){
				flumeClient.send(getEventFromOp(getOp(operation)));
			}
		}catch(Exception e){
			retVal = Status.ABEND;
			
			LOG.error("there was an error during operation added: " + e.getMessage());
		}
		
		return retVal;
	}

	protected Op getOp(DsOperation operation) {
		final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());
		
		return new Op(operation, tMeta, getConfig());
	}

	protected Status informOperationAdded(DsEvent event, DsTransaction transaction, DsOperation operation) {
		return super.operationAdded(event, transaction, operation);
	}

	private Event getEventFromOp(Op op) throws ParseException, IOException{
        if (!getOpType(op).isInsert())
            return null;
		
        for (Col col : op)
            recordBuilder.set(col.getName(), TypeConverter.toAvro(col));
        
		GenericRecord record = recordBuilder.build();
		
		return EventBuilder.withBody(serialize(record, schema));
	}

	protected OpType getOpType(Op op) {
		return op.getOperationType();
	}

	private byte[] serialize(GenericRecord record, Schema schema) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		
		ReflectDatumWriter<GenericRecord> writer = new ReflectDatumWriter<GenericRecord>(schema);
		
		writer.write(record, encoder);
		encoder.flush();
		
		return out.toByteArray();
	}

	@Override
	public Status transactionCommit(DsEvent event, DsTransaction transaction) {
		Status retVal = informTransactionCommit(event, transaction);
		
		try{
			if(!isOperationMode()){
				Tx ops = getOps(transaction);
				List<Event> events = Lists.newLinkedList();
				for (Op op : ops)
					events.add(getEventFromOp(op));
				
				flumeClient.send(events);
			}
		}catch(Exception e){
			retVal = Status.ABEND;
			
			LOG.error("there was an error during commit: " + e.getMessage());
		}
			
		return retVal;
	}
	
	protected Tx getOps(DsTransaction transaction) {
		return new Tx(transaction, getMetaData(), getConfig());
	}

	protected Status informTransactionCommit(DsEvent event, DsTransaction transaction) {
		return super.transactionCommit(event, transaction);
	}

	@Override
	public Status transactionRollback(DsEvent e, DsTransaction tx) {
		Status retVal = informTransactionRollBack(e, tx);
		
		return retVal;
	}

	protected Status informTransactionRollBack(DsEvent e, DsTransaction tx) {
		return super.transactionRollback(e, tx);
	}

	@Override
	public String reportStatus() {
		return "status reported";
	}
	
	@Override
	public void destroy() {
		flumeClient.disconnect();
		
		super.destroy();
	}
	
    public void setFlumeHost(String flumeHost) {
        flumeClient.setHost(flumeHost);
    }

    public void setFlumePort(String flumePort) {
    	flumeClient.setPort(Integer.parseInt(flumePort));
    }
    
    public void setSourceTable(String sourceTable) {
    	this.sourceTable = sourceTable;
    }
    
    /**
     * Must be specified with the following syntax:
     * dataset:hdfs:/<path>/<namespace>/<dataset-name>
     * 
     * The Hadoop configuration files should be on your classpath
     * Otherwise you can use the following syntax:
     * dataset:hdfs://<host>[:port]/<path>/<namespace>/<dataset-name>
     * 
     * @param uri
     */
    public void setDatasetURI(String uri){
    	try {
			this.dataset_uri = new URI(uri);
		} catch (URISyntaxException e) {
			Throwables.propagate(e);
		}
    }

	public FlumeClient getFlumeClient() {
		return new FlumeClient();
	}
	
}
