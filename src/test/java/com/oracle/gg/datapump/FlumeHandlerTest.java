package com.oracle.gg.datapump;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeClient;
import org.junit.Before;
import org.junit.Test;

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
import com.google.common.collect.Lists;

public class FlumeHandlerTest {
	
	private FlumeHandler handler;
	private FlumeClient flumeClient;
	
	@Before
	public void beforeTest(){
		handler = spy(new FlumeHandler());
		
		doNothing().when(handler).informInit((DsConfiguration) any(), (DsMetaData) any());
		doNothing().when(handler).informTransactionBegin((DsEvent) any(), (DsTransaction) any());
		doReturn(Status.OK).when(handler).informOperationAdded((DsEvent) any(), (DsTransaction) any(), (DsOperation) any());
		doReturn(Status.OK).when(handler).informTransactionCommit((DsEvent) any(), (DsTransaction) any());
		doReturn(Status.OK).when(handler).informTransactionRollBack((DsEvent) any(), (DsTransaction) any());
		
		flumeClient = spy(new FlumeClient());
		handler.setFlumeClient(flumeClient);
		
		handler.setFlumeHost("itrac901.cern.ch");
		handler.setFlumePort("41444");
		handler.setDatasetURI("dataset:file:target/test_repo/sample_data_numeric");
		
		handler.init(null, null);
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
		when(handler.isOperationMode()).thenReturn(false);
		doReturn(OpType.DO_INSERT).when(handler).getOpType((Op) any());
		
		Tx ops_ = mock(Tx.class);
		List<Op> l = Lists.newLinkedList();
		for (int i = 0; i < 10; i++)
			l.add(getMockedOp());
		doReturn(l.iterator()).when(ops_).iterator();
		doReturn(ops_).when(handler).getOps((DsTransaction) any());
		
		DsOperation op = mock(DsOperation.class);
		when(op.toString()).thenReturn("op");
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionRollback(null, null);
		
		verify(flumeClient, times(0)).send(anyListOf(Event.class));
		verify(flumeClient, times(0)).send((Event) any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionBegin(null, null);
		
		verify(flumeClient, times(0)).send(anyListOf(Event.class));
		verify(flumeClient, times(0)).send((Event) any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		handler.transactionCommit(null, null);
		
		verify(flumeClient, times(1)).send(anyListOf(Event.class));
		verify(flumeClient, times(0)).send((Event) any());
	}
	
	@Test
	public void operationalMode() throws EventDeliveryException {
		Op op_ = getMockedOp();
		
		doReturn(op_).when(handler).getOp((DsOperation) any());
		
		doReturn(OpType.DO_INSERT).when(handler).getOpType((Op) any());
		when(handler.isOperationMode()).thenReturn(true);
		
		DsOperation op = mock(DsOperation.class);
		when(op.toString()).thenReturn("op");
		
		handler.init(null, null);
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, null);
		
		verify(flumeClient, times(10)).send((Event) any());
	}
	
	private Op getMockedOp() {
		Op op = mock(Op.class);
		doReturn("operational mode").when(op).toString();
		
		List<Col> l = Lists.newLinkedList();
		Col col = TypeConverterTests.getMockedCol("VARIABLE_ID", String.valueOf((int)(Math.random() * 23)), Types.INTEGER);
		l.add(col);
		
		col = TypeConverterTests.getMockedCol("UTC_STAMP", "2015-03-20:11:52:46.335853000", Types.TIMESTAMP);
		l.add(col);
		
		col = TypeConverterTests.getMockedCol("VALUE", String.valueOf(Math.random() * 24123f), Types.DOUBLE);
		l.add(col);

		doReturn(l.iterator()).when(op).iterator();
		
		return op;
	}

	@Test
	public void sendEvent() {
		doReturn(OpType.DO_INSERT).when(handler).getOpType((Op) any());
		
		doReturn(false).when(handler).isOperationMode();
		
		handler.init(null, null);
		
		float num_events = 1000;
		
		long t = System.currentTimeMillis();
		
		for (int i = 0; i < num_events; i++){
			Op op_ = getMockedOp();
			doReturn(op_).when(handler).getOp((DsOperation) any());
			
			handler.operationAdded(null, null, null);
		}
		
		System.out.println("ms per event: " + (System.currentTimeMillis() - t) / num_events);
		
		handler.destroy();
	}

	@Test
	public void sendEvents() {
		doReturn(OpType.DO_INSERT).when(handler).getOpType((Op) any());
		
		doReturn(false).when(handler).isOperationMode();
		
		handler.init(null, null);
		
		when(handler.isOperationMode()).thenReturn(true);
		
		float num_events = 10000;

		flumeClient.setBanchSize(100000);
		flumeClient.disconnect();
		flumeClient.connect();
		
		Tx ops_ = mock(Tx.class);
		List<Op> l = Lists.newLinkedList();
		for (int i = 0; i < num_events; i++)
			l.add(getMockedOp());
		doReturn(l.iterator()).when(ops_).iterator();
		doReturn(ops_).when(handler).getOps((DsTransaction) any());
		
		long t = System.currentTimeMillis();
		
		handler.transactionCommit(null, null);
		
		System.out.println("ms per event: " + (System.currentTimeMillis() - t) / num_events);
		
		handler.destroy();
	}
}
