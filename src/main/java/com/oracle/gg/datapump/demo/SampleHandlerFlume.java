package com.oracle.gg.datapump.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.adapt.*;
import com.goldengate.atg.datasource.meta.*;
import com.goldengate.atg.util.GGException;

import java.util.*;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

import static com.goldengate.atg.datasource.GGDataSource.Status;

public class SampleHandlerFlume extends AbstractHandler {
    final private static Logger logger = LoggerFactory.getLogger(SampleHandlerFlume.class);

    private static String TABLE_HEADER = "table";
    private static String SCHEMA_HEADER = "schema";
    private String recordDelimiter = "\1"; // Default Hive delimiter

    // Flume
    private String flumeHost = "localhost";
    private int flumePort = 41414;
    private RpcClient flumeClient;

    public SampleHandlerFlume() {
        super(TxOpMode.op); // define default mode for handler
    }

    public void init(DsConfiguration conf, DsMetaData metaData) {
        logger.info("Initializing handler");
        super.init(conf, metaData); // always call 'super'

        // Setup the RPC connection
        flumeClient = RpcClientFactory.getDefaultInstance(flumeHost, flumePort);
    }

    public void insertFlume(String data, String table, String schema) {
        try {
            // Create a Flume Event object that encapsulates the sample data
            Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
            Map<String, String> headers = event.getHeaders();
            headers.put(SCHEMA_HEADER, schema);
            headers.put(TABLE_HEADER, table);
            event.setHeaders(headers);
            flumeClient.append(event);
        } catch (EventDeliveryException ex) {
            logger.error("Caught Exception", ex);
            throw new GGException("Exception in insertFlume", ex);
        }
    }


    // Property setter methods.

    public void setRecordDelimiter(String delim) {
        logger.info("set record delimiter: " + delim);
        this.recordDelimiter = delim;
    }


    public void setFlumeHost(String flumeHost) {
        this.flumeHost = flumeHost;
    }


    public void setFlumePort(String flumePort) {
        this.flumePort = Integer.parseInt(flumePort);
    }


    public Status operationAdded(DsEvent e, DsTransaction transaction,
                                 DsOperation operation) {
        logger.debug("operationAdded");
        super.operationAdded(e, transaction, operation);

        // Tx/Op/Col adapters wrap metadata & values behind a single, simple
        // interface if using the DataSourceListener API (via AbstractHandler).

        final TableMetaData tMeta =
            getMetaData().getTableMetaData(operation.getTableName());
        final Op op = new Op(operation, tMeta, getConfig());

        processOp(op); // process data...
        return Status.OK;
    }


    private void processOp(Op op) {
        logger.debug("processOp:  op={1}", op.getPosition());

        DsOperation.OpType opType = op.getOperationType();
        if (!opType.isInsert())
            return;

        StringBuffer strBuf = new StringBuffer("");

        logger.debug("  ===> Processing operation: " + ", table='" +
                     op.getTableName() + "'" + ", pos=" + op.getPosition() +
                     ", ts=" + op.getTimestamp());
        TableName tname = op.getTableName();
        TableMetaData tMeta = getMetaData().getTableMetaData(tname);
        int i = 0;
        boolean first = true;
        for (Col c : op) {
            if (first)
                first = false;
            else
                strBuf.append(recordDelimiter);
            logger.debug("Col: " + tMeta.getColumnName(i) + "=" + c + "," +
                         c.getValue());
            strBuf.append(c.getValue());
            i++;
        }

        String tableName = op.getTableName().getShortName().toLowerCase();
        String schemaName = op.getTableName().getSchemaName();

        insertFlume(strBuf.toString(), tableName, schemaName);
    }


    public String reportStatus() {
        logger.debug("reportStatus");
        String s = "Status report";
        return s;
    }
}
