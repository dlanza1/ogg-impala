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
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.google.common.collect.Lists;

public class FlumeHandler extends AbstractHandler {
	final private static Logger LOG = LoggerFactory.getLogger(FlumeHandler.class);
	
	protected RpcClient flumeClient;
	protected Integer flumePort;
	protected String flumeHost;
	
	protected List<Event> flumeEvents;
	
	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		initHandler(conf, metaData); 
		
        LOG.info("Initializing handler");

        flumeClient = getFlumeClient(flumeHost, flumePort);
        
        if(!isOperationMode())
        	flumeEvents = Lists.newArrayList();
	}
	
	protected RpcClient getFlumeClient(String flumeHost2, Integer flumePort2) {
		return RpcClientFactory.getDefaultInstance(flumeHost, flumePort);
	}

	protected void initHandler(DsConfiguration conf, DsMetaData metaData) {
		super.init(conf, metaData);
	}

	@Override
	public Status transactionBegin(DsEvent e, DsTransaction tx) {
		if(!isOperationMode())
			flumeEvents.clear();
		
		return Status.OK;
	}
	
	@Override
	public Status operationAdded(DsEvent dsEvent, DsTransaction dsTx, DsOperation dsOp) {
		Event event = EventBuilder.withBody(dsOp.toString(), Charset.forName("UTF-8"));

		try {
			if(isOperationMode())
				flumeClient.append(event);
			else
				flumeEvents.add(event);
		} catch (EventDeliveryException e) {
			e.printStackTrace();
			
			return Status.ABORT;
		}
		
		return Status.OK;
	}
	
	@Override
	public Status transactionCommit(DsEvent dsEvent, DsTransaction dsTx) {
		
		try {
			if(!isOperationMode())
				flumeClient.appendBatch(flumeEvents);
		} catch (EventDeliveryException e) {
			e.printStackTrace();
			
			return Status.ABORT;
		} finally {
			flumeEvents.clear();
		}
			
		return Status.OK;
	}
	
	@Override
	public Status transactionRollback(DsEvent e, DsTransaction tx) {
		if(!isOperationMode())
			flumeEvents.clear();
		
		return Status.OK;
	}
	
	@Override
	public String reportStatus() {
		return "Flume events = " + flumeEvents.size();
	}
	
	@Override
	public void destroy() {
		super.destroy();
		
		if(!isOperationMode())
			flumeEvents.clear();
		
		flumeClient.close();
	}
	
    public void setFlumeHost(String flumeHost) {
        this.flumeHost = flumeHost;
    }

    public void setFlumePort(String flumePort) {
        this.flumePort = Integer.parseInt(flumePort);
    }
	
}
