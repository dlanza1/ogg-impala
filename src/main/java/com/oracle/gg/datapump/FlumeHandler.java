package com.oracle.gg.datapump;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.meta.DsMetaData;

public class FlumeHandler extends AbstractHandler {
	
	@Override
	public void init(DsConfiguration conf, DsMetaData metaData) {
		super.init(conf, metaData);
	}
	
	@Override
	public Status transactionBegin(DsEvent e, DsTransaction tx) {
		return super.transactionBegin(e, tx);
	}
	
	@Override
	public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation op) {
		isOperationMode();
		
		return super.operationAdded(e, tx, op);
	}
	
	@Override
	public Status transactionCommit(DsEvent e, DsTransaction tx) {
		return super.transactionCommit(e, tx);
	}
	
	@Override
	public Status transactionRollback(DsEvent e, DsTransaction tx) {
		return super.transactionRollback(e, tx);
	}
	
	@Override
	public String reportStatus() {
		return super.reportStatus();
	}
	
	@Override
	public void destroy() {
		super.destroy();
	}
	
}
