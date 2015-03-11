package com.oracle.gg.datapump;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.google.common.collect.Lists;

public class FlumeHandler extends AbstractHandler {
	final private static Logger LOG = LoggerFactory.getLogger(FlumeHandler.class);
	
	protected RpcClient flumeClient;
	protected Integer flumePort;
	protected String flumeHost;
	
	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		LOG.info("Initializing handler...");
		
		if(flumeHost == null)
			throw new RuntimeException("the Flume host must be specified in the properties file");
		if(flumePort == null)
			throw new RuntimeException("the Flume port must be specified in the properties file");
		
		informInit(conf, metaData); 
		
		flumeClient = getFlumeClient();
		
		LOG.info("Handler was inicialized with Flume client (host=" + flumeHost + ", port=" + flumePort + ")");
	}
	
	protected void informInit(DsConfiguration conf, DsMetaData metaData) {
		super.init(conf, metaData);
	}
	
	protected RpcClient getFlumeClient() {
		return RpcClientFactory.getDefaultInstance(flumeHost, flumePort);
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
		informOperationAdded(event, transaction, operation);
		
		if(isOperationMode()){
			final Op op = getOp(operation);
			
			try {
				flumeClient.append(getEventFromOp(op));
			} catch (EventDeliveryException e) {
				e.printStackTrace();
				
				return Status.ABEND;
			}
		}
		
		return Status.OK;
	}
	
	protected Op getOp(DsOperation operation) {
		final TableMetaData tMeta = getMetaData().getTableMetaData(operation.getTableName());
		return new Op(operation, tMeta, getConfig());
	}

	protected void informOperationAdded(DsEvent event, DsTransaction transaction, DsOperation operation) {
		super.operationAdded(event, transaction, operation);
	}

	private static Event getEventFromOp(Op op){
		Event event = EventBuilder.withBody(op.toString(), Charset.forName("UTF-8"));
		
		return event;
	}

	@Override
	public Status transactionCommit(DsEvent event, DsTransaction transaction) {
		informTransactionCommit(event, transaction);
		
		if(!isOperationMode()){
			Tx ops = getOps(transaction);
			List<Event> events = Lists.newLinkedList();
			for (Op op : ops)
				events.add(getEventFromOp(op));
			
			try {
				flumeClient.appendBatch(events);
			} catch (EventDeliveryException e) {
				e.printStackTrace();
				
				return Status.ABORT;
			}
		}
			
		return Status.OK;
	}
	
	protected Tx getOps(DsTransaction transaction) {
		return new Tx(transaction, getMetaData(), getConfig());
	}

	protected void informTransactionCommit(DsEvent event, DsTransaction transaction) {
		super.transactionCommit(event, transaction);
	}

	@Override
	public Status transactionRollback(DsEvent e, DsTransaction tx) {
		informTransactionRollBack(e, tx);
		
		return Status.OK;
	}

	protected void informTransactionRollBack(DsEvent e, DsTransaction tx) {
		super.transactionRollback(e, tx);
	}

	@Override
	public String reportStatus() {
		return "status reported";
	}
	
	@Override
	public void destroy() {
		flumeClient.close();
		
		super.destroy();
	}
	
    public void setFlumeHost(String flumeHost) {
        this.flumeHost = flumeHost;
    }

    public void setFlumePort(String flumePort) {
        this.flumePort = Integer.parseInt(flumePort);
    }
	
}
