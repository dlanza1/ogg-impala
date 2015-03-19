package com.oracle.gg.datapump;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

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
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;

public class TestHandler extends AbstractHandler {
	final private static Logger LOG = Logger.getLogger(TestHandler.class);
	
	BufferedWriter writer;
	
	public TestHandler() {
	}
	
	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		LOG.info("Initializing handler...");
		
		informInit(conf, metaData); 
		
		File logFile = new File("test-handler.out");

        try {
			writer = new BufferedWriter(new FileWriter(logFile));
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("there was an error openning the output file");
		}
		
		try {
			LOG.info("Handler was inicialized, wirting in: " + logFile.getCanonicalPath());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("there was an error showing the canonical path of the output file");
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
				writer.write("op added\n");
				print(getOp(operation));
				writer.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				writer.flush();
			} catch (IOException e) {}
		}
		
//		retVal = Status.ABEND;
//		LOG.error("there was an error during operation added: " + e.getMessage());
		
		
		return retVal;
	}

	protected Op getOp(DsOperation operation) {
		final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());

		return new Op(operation, tMeta, getConfig());
	}

	protected Status informOperationAdded(DsEvent event, DsTransaction transaction, DsOperation operation) {
		return super.operationAdded(event, transaction, operation);
	}

	private void print(Op op) throws IOException{
		writer.write("op, cols:\n");
		
        for (Col col : op){
        	writer.write(print(col));
        }
        
        writer.write("\n");
	}

	private String print(Col col) throws IOException {
		String out = new String();
		
		writer.write("Name: " + col.getName());
		writer.write("  ValueString: " + col.getAfter().getValue());
		try {
			Object value = TypeConverter.toAvro(col);
			writer.write("  TypeConverter: " + value.getClass() + " " + value);
		} catch (ParseException e) {
			e.printStackTrace();
			writer.write("  TypeConverter: parseError-" + e.getMessage());
		} catch (Exception e) {
			writer.write("  TypeConverter: error-" + e.getMessage());
		}
		try {
			Object value = TypeConverter.toOracleSQLType(col);
			writer.write("  oracle.sql: " + value.getClass() + " " + value);
		} catch (ParseException e) {
			e.printStackTrace();
			writer.write("  oracle.sql: parseError-" + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Information regarding oracle.sql: error", e);
			writer.write("  oracle.sql: error" + e.getMessage());
		}
		
		return out.concat("\n");
	}

	protected OpType getOpType(Op op) {
		return op.getOperationType();
	}

	@Override
	public Status transactionCommit(DsEvent event, DsTransaction transaction) {
		Status retVal = informTransactionCommit(event, transaction);
		
		try{
			if(!isOperationMode()){
				writer.write("trans commit\n");
				
				Tx ops = getOps(transaction);
				for (Op op : ops)
					print(op);
			
			}
		}catch(Exception e){
			retVal = Status.ABEND;
			
			LOG.error("there was an error during commit: " + e.getMessage());
		}finally{
			try {
				writer.flush();
			} catch (IOException e) {}
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
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		super.destroy();
	}
	
}
