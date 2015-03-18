package com.oracle.gg.datapump;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeClient {
	final private static Logger LOG = LoggerFactory.getLogger(FlumeClient.class);

	protected RpcClient rpcClient;
	
	protected String host;
	protected Integer port;
	private Integer batchSize;
	
	public FlumeClient() {
		this.host = null;
		this.port = 0;
		this.batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
	}

	public void connect() {
		if(host == null)
			throw new RuntimeException("the Flume host must be specified in the properties file");
		if(port == null)
			throw new RuntimeException("the Flume port must be specified in the properties file");
		
		rpcClient = RpcClientFactory.getDefaultInstance(host, port, batchSize);
		
		LOG.info("Flume client has been connected (host=" + host + ", port=" + port + ")");
	}

	public void disconnect() {
		LOG.info("Flume client has been disconnected");
		
		rpcClient.close();
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
	
	public void setBanchSize(Integer banchSize) {
		this.batchSize = banchSize;
	}

	public void send(Event event) throws EventDeliveryException {
		if(event == null)
			return;
		
		try {
			rpcClient.append(event);
		} catch (EventDeliveryException e) {
			reset();
			
			throw new EventDeliveryException(e);
		}
	}

	private void reset() {
		LOG.info("Resetting connection...");
		
		connect();
		rpcClient = null;
		disconnect();
	}

	public void send(List<Event> events) throws EventDeliveryException {
		try {
			if(events.size() > rpcClient.getBatchSize())
				LOG.warn("appending a batch of events (" + events.size()
						+ ") greater than the configured banch size (" + rpcClient.getBatchSize() + "), "
						+ "so the likelihood of duplicate Events being stored will increase. "
						+ "Maybe you should increase the batch size.");
			
			rpcClient.appendBatch(events);
		} catch (EventDeliveryException e) {
			reset();
			
			throw new EventDeliveryException(e);
		}
	}
	
}
