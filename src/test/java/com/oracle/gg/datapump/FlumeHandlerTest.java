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

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.junit.Before;
import org.junit.Test;

import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsOperation.OpType;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.test.DsTestUtils;
import com.google.common.collect.Lists;

public class FlumeHandlerTest {
	
	private FlumeHandler handler;
	private FlumeClient flumeClient;
	
	@Before
	public void beforeTest(){
		handler = spy(new FlumeHandler());
		
		doNothing().when(handler).informInit(null, null);
		doNothing().when(handler).informTransactionBegin((DsEvent) any(), (DsTransaction) any());
		doNothing().when(handler).informOperationAdded((DsEvent) any(), (DsTransaction) any(), (DsOperation) any());
		doNothing().when(handler).informTransactionCommit((DsEvent) any(), (DsTransaction) any());
		doNothing().when(handler).informTransactionRollBack((DsEvent) any(), (DsTransaction) any());
		
//		flumeClient = spy(new FlumeClient());
//		doReturn(flumeClient).when(handler).getFlumeClient();
		
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

	//@Test
	public void noOperationalMode() throws EventDeliveryException {
		when(handler.isOperationMode()).thenReturn(false);
		
		Tx ops_ = mock(Tx.class);
		Op op_ = mock(Op.class);
		doReturn("string").when(op_).toString();
		doReturn(null).when(op_).getOperationType();
		List<Op> l = Lists.newLinkedList();
		for (int i = 0; i < 10; i++)
			l.add(op_);
		doReturn(l.iterator()).when(ops_).iterator();
		doReturn(ops_).when(handler).getOps((DsTransaction) any());
		
		handler.init(null, null);
		
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
		
		verify(flumeClient, times(0)).send((Event) any());
	}
	
	private Op getMockedOp() {
		Op op = mock(Op.class);
		doReturn("operational mode").when(op).toString();
		
		List<Col> l = Lists.newLinkedList();
		Col col = mock(Col.class);
		doReturn("VARIABLE_ID").when(col).getName();
		doReturn(1).when(col).getData().getBinary();
		doReturn(false).when(col).isValueNull();
		doReturn(java.sql.Types.TIMESTAMP).when(col).getDataType().getJDBCType();
		l.add(col);
		
		col = mock(Col.class);
		doReturn("UTC_STAMP").when(col).getName();
		doReturn("1").when(col).getValue();
		doReturn(false).when(col).isValueNull();
		doReturn(java.sql.Types.TIMESTAMP).when(col).getDataType().getJDBCType();
		l.add(col);
		
		col = mock(Col.class);
		doReturn("VALUE").when(col).getName();
		doReturn("1").when(col).getValue();
		doReturn(false).when(col).isValueNull();
		doReturn(java.sql.Types.TIMESTAMP).when(col).getDataType().getJDBCType();
		l.add(col);

		doReturn(l.iterator()).when(op).iterator();
		
		return op;
	}

	@Test
	public void sendEvents() {
		flumeClient = spy(new FlumeClient());
		doReturn(flumeClient).when(handler).getFlumeClient();
		
		handler.init(null, null);
		
		when(handler.isOperationMode()).thenReturn(true);
		
		for (int i = 0; i < 10; i++){
			Op op_ = mock(Op.class);
			doReturn("operation=" + 1).when(op_).toString();
			doReturn(OpType.DO_INSERT).when(op_).getOperationType();
			doReturn(op_).when(handler).getOp((DsOperation) any());
			
			handler.operationAdded(null, null, null);
		}
		
		handler.destroy();
	}
	
	@Test
	public void gg() {
		DsTestUtils gg_test = new DsTestUtils();

	}

}
