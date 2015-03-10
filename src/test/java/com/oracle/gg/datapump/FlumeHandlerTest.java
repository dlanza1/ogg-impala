package com.oracle.gg.datapump;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.goldengate.atg.datasource.DsOperation;

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
		Mockito.doNothing().when(handler).initHandler(null, null);
		
		flumeClient = Mockito.mock(RpcClient.class);
		Mockito.doReturn(flumeClient).when(handler).getFlumeClient(null, null);
	}
	
	class EventMatcher extends ArgumentMatcher<Event>{
		@Override
		public boolean matches(Object argument) {
			return true;
		}
	}
	
	class ListMaptcher extends ArgumentMatcher<List<Event>>{
		@Override
		public boolean matches(Object argument) {
			return true;
		}
	}

	@Test
	public void noOperationalMode() throws EventDeliveryException {
		Mockito.when(handler.isOperationMode()).thenReturn(false);
		
		handler.init(null, null);
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		Assert.assertEquals(10, handler.flumeEvents.size());
		handler.transactionRollback(null, null);
		Assert.assertEquals(0, handler.flumeEvents.size());
		
		Mockito.verify(flumeClient, Mockito.times(0)).appendBatch(Mockito.anyListOf(Event.class));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		Assert.assertEquals(10, handler.flumeEvents.size());
		handler.transactionBegin(null, null);
		Assert.assertEquals(0, handler.flumeEvents.size());
		
		Mockito.verify(flumeClient, Mockito.times(0)).appendBatch(Mockito.anyListOf(Event.class));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		Assert.assertEquals(10, handler.flumeEvents.size());
		handler.transactionCommit(null, null);
		Assert.assertEquals(0, handler.flumeEvents.size());
		
		Mockito.verify(flumeClient, Mockito.times(1)).appendBatch(Mockito.anyListOf(Event.class));
		Mockito.verify(flumeClient, Mockito.times(0)).append((Event) Mockito.any());
	}
	
	@Test
	public void operationalMode() throws EventDeliveryException {
		Mockito.when(handler.isOperationMode()).thenReturn(true);
		
		handler.init(null, null);
		
		Assert.assertNull(handler.flumeEvents);
		
		for (int i = 0; i < 10; i++)
			handler.operationAdded(null, null, op);
		
		Assert.assertNull(handler.flumeEvents);
		
		Mockito.verify(flumeClient, Mockito.times(10)).append((Event) Mockito.any());
	}
	
}
