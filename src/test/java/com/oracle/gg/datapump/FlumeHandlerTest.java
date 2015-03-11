package com.oracle.gg.datapump;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.google.common.collect.Lists;

public class FlumeHandlerTest {
	
	private static DsOperation op;
	private static FlumeHandler handler;
	private static RpcClient flumeClient;
	
	@BeforeClass
	public static void setUp(){
		op = Mockito.mock(DsOperation.class);
		Mockito.when(op.toString()).thenReturn("op");
	}
	
	@Before
	public void beforeTest(){
		handler = Mockito.spy(new FlumeHandler());
		Mockito.doNothing().when(handler).informInit(null, null);
		Mockito.doNothing().when(handler).informTransactionBegin((DsEvent) Mockito.any(), 
				(DsTransaction) Mockito.any());
		Mockito.doNothing().when(handler).informOperationAdded((DsEvent) Mockito.any(), 
				(DsTransaction) Mockito.any(), 
				(DsOperation) Mockito.any());
		Mockito.doNothing().when(handler).informTransactionCommit((DsEvent) Mockito.any(), 
				(DsTransaction) Mockito.any());
		Mockito.doNothing().when(handler).informTransactionRollBack((DsEvent) Mockito.any(), 
				(DsTransaction) Mockito.any());
		
		Op op_ = Mockito.mock(Op.class);
		Mockito.doReturn("string").when(op_).toString();
		Mockito.doReturn(op_).when(handler).getOp((DsOperation) Mockito.any());
		
		flumeClient = Mockito.mock(RpcClient.class);
		Mockito.doReturn(flumeClient).when(handler).getFlumeClient();
		
		handler.setFlumeHost("itrac901.cern.ch");
		handler.setFlumePort("41444");
	}
	
	@Test
	public void init() throws EventDeliveryException {
		handler.init(null, null);
	}

	@Test(expected=RuntimeException.class)
	public void initWithNullHostAndPort() throws EventDeliveryException {
		handler.setFlumeHost(null);
		handler.setFlumePort(null);
		handler.init(null, null);
	}
	
	@Test(expected=RuntimeException.class)
	public void initWithNullHost() throws EventDeliveryException {
		handler.setFlumeHost(null);
		handler.init(null, null);
	}
	
	@Test(expected=RuntimeException.class)
	public void initWithNullPort() throws EventDeliveryException {
		handler.setFlumePort(null);
		handler.init(null, null);
	}

	@Test
	public void noOperationalMode() throws EventDeliveryException {
		Mockito.when(handler.isOperationMode()).thenReturn(false);
		
		Tx ops_ = Mockito.mock(Tx.class);
		Op op_ = Mockito.mock(Op.class);
		Mockito.doReturn("string").when(op_).toString();
		List<Op> l = Lists.newLinkedList();
		for (int i = 0; i < 10; i++)
			l.add(op_);
		Mockito.doReturn(l.iterator()).when(ops_).iterator();
		Mockito.doReturn(ops_).when(handler).getOps((DsTransaction) Mockito.any());
		
		handler.init(null, null);
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionRollback(null, null);
		
		Mockito.verify(flumeClient, Mockito.times(0)).appendBatch(Mockito.anyListOf(Event.class));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionBegin(null, null);
		
		Mockito.verify(flumeClient, Mockito.times(0)).appendBatch(Mockito.anyListOf(Event.class));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionCommit(null, null);
		
		Mockito.verify(flumeClient, Mockito.times(1)).appendBatch(Mockito.argThat(MyMatchers.list(Event.class, 10)));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
	}
	
	@Test
	public void operationalMode() throws EventDeliveryException {
		Mockito.when(handler.isOperationMode()).thenReturn(true);
		
		handler.init(null, null);
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		Mockito.verify(flumeClient, Mockito.times(10)).append((Event) Mockito.any());
	}
	
	@Test
	public void sendEvents() {
		flumeClient = Mockito.spy(RpcClientFactory.getDefaultInstance("itrac901.cern.ch", 41444));
		Mockito.doReturn(flumeClient).when(handler).getFlumeClient();
		
		handler.init(null, null);
		
		Mockito.when(handler.isOperationMode()).thenReturn(true);
		
		for (int i = 0; i < 10; i++){
			Op op_ = Mockito.mock(Op.class);
			Mockito.doReturn("operation=" + 1).when(op_).toString();
			Mockito.doReturn(op_).when(handler).getOp((DsOperation) Mockito.any());
			
			handler.operationAdded(null, null, op);
		}
		
		handler.destroy();
	}

}
