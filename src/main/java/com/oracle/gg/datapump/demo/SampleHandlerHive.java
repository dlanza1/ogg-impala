package com.oracle.gg.datapump.demo;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.TxOpMode;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import com.goldengate.atg.util.ConfigException;

public class SampleHandlerHive extends AbstractHandler {
	final private static Logger logger = LoggerFactory
			.getLogger(SampleHandlerHive.class);

	/** count total number of operations */
	private long numOps = 0;
	/** count total number of transactions */
	private long numTxs = 0;

	private String regularFileName = "default_hive_debug.txt";
	private String hdfsFileName = "default_hive.txt";
	private String recordDelimiter = ":";
	private PrintWriter out;

	/* Hadoop variables */

	private FSDataOutputStream hadoopOutputStream;

	public SampleHandlerHive() {
		super(TxOpMode.op); // define default mode for handler
		logger.info("Created handler: default mode=" + getMode()
				+ ", default hive filename=" + hdfsFileName);
	}

	public void init(DsConfiguration conf, DsMetaData metaData) {
		logger.info("Initializing handler: mode=" + getMode()
				+ ", Hive filename=" + hdfsFileName);
		super.init(conf, metaData); // always call 'super'

		// Initialize regular file
		try {
			out = new PrintWriter(new FileWriter(regularFileName));
		} catch (IOException ioe) {
			throw new ConfigException("Can't initialize file, "
					+ regularFileName, ioe);
		}

		// Initialize HDFS
		try {
			FileSystem hadoopFs;
			Path hadoopFilePath;
			Configuration hadoopConf = new Configuration();
			hadoopFs = FileSystem.get(hadoopConf);
			Path homeDirPath = hadoopFs.getHomeDirectory();
			Path workingDirPath = hadoopFs.getWorkingDirectory();
			hadoopFilePath = new Path(hdfsFileName);
			if (hadoopFs.exists(hadoopFilePath)) {
				hadoopOutputStream = hadoopFs.append(hadoopFilePath);
			} else {
				hadoopOutputStream = hadoopFs.create(hadoopFilePath);
			}
		} catch (IOException ioe) {
			logger.info("Can't initialize HDFS file, " + hdfsFileName, ioe);
			throw new ConfigException("Can't initialize HDFS file, "
					+ hdfsFileName, ioe);
		}

	}

	// Property setter methods.

	public void setHDFSFileName(String filename) {
		logger.info("set HDFS filename: " + filename);
		this.hdfsFileName = filename;
	}

	public void setRegularFileName(String filename) {
		logger.info("set regular filename: " + filename);
		this.regularFileName = filename;
	}

	public void setRecordDelimiter(String delim) {
		logger.info("set record delimiter: " + delim);
		this.recordDelimiter = delim;
	}

	public Status transactionBegin(DsEvent e, DsTransaction tx) {
		logger.debug("transactionBegin");
		super.transactionBegin(e, tx);
		String eventFormat;
		eventFormat = String.format(
				"Received begin tx event, numTx=%d : position=%s%n", numTxs,
				tx.getTranID());
		out.println(eventFormat);
		return Status.OK;
	}

	public Status operationAdded(DsEvent e, DsTransaction transaction,
			DsOperation operation) {
		logger.debug("operationAdded");
		super.operationAdded(e, transaction, operation);

		// Tx/Op/Col adapters wrap metadata & values behind a single, simple
		// interface if using the DataSourceListener API (via AbstractHandler).

		final Tx tx = new Tx(transaction, getMetaData(), getConfig());
		final TableMetaData tMeta = getMetaData().getTableMetaData(
				operation.getTableName());
		final Op op = new Op(operation, tMeta, getConfig());

		out.println("  Received operation: table='" + op.getTableName() + "'"
				+ ", pos=" + op.getPosition() + " (total_ops= "
				+ tx.getTotalOps() + ", buffered=" + tx.getSize() + ")"
				+ ", ts=" + op.getTimestamp());

		if (isOperationMode()) {
			processOp(tx, op); // process data...
		}
		return Status.OK;
	}

	public Status transactionCommit(DsEvent e, DsTransaction transaction) {
		logger.debug("transactionCommit");
		super.transactionCommit(e, transaction);

		numTxs++;

		Tx tx = new Tx(transaction, getMetaData(), getConfig());

		if (!isOperationMode()) {
			for (Op op : tx) {
				processOp(tx, op); // process data...
			}
		}

		out.println("  Received commit event, tx #" + numTxs + ": " + ", pos="
				+ tx.getTranID() + " (total_ops= " + tx.getTotalOps()
				+ ", buffered=" + tx.getSize() + ")" + ", ts="
				+ tx.getTimestamp() + ")");
		out.flush();

		return Status.OK;
	}

	private void processOp(Tx currentTx, Op op) {
		if (logger.isDebugEnabled())
			logger.debug("processOp: tx=" + currentTx.getPosition() + ", op="
					+ op.getPosition());
		numOps++;

		try {
			out.println("  ===> Processing operation: " + ", table='"
					+ op.getTableName() + "'" + ", pos=" + op.getPosition()
					+ " (total_ops= " + currentTx.getTotalOps() + ", buffered="
					+ currentTx.getSize() + ")" + ", ts=" + op.getTimestamp());
			TableName tname = op.getTableName();
			TableMetaData tMeta = getMetaData().getTableMetaData(tname);
			int i = 0;
			for (Col c : op) {
				out.println("Col: " + tMeta.getColumnName(i) + "=" + c);
				hadoopOutputStream.writeChars(c + recordDelimiter);
				i++;
			}
			out.println();
			hadoopOutputStream.writeUTF("\n");
			hadoopOutputStream.sync();
		} catch (java.io.IOException ioe) {
		}
	}

	public Status metaDataChanged(DsEvent e, DsMetaData meta) {
		logger.debug("metaDataChanged");
		super.metaDataChanged(e, meta);
		out.println("Received metadata event: " + e + "; current tables: "
				+ meta.getTableNames().size());
		return Status.OK;
	}

	public void destroy() {
		logger.debug("destroy");
		// ... do cleanup ...
		out.println("Closing file... " + reportStatus());
		out.flush();
		out.close();
		try {
			hadoopOutputStream.close();
		} catch (IOException ioe) {

		}
		super.destroy();
	}

	public String reportStatus() {
		logger.debug("reportStatus");
		String s = "Status report: file=" + hdfsFileName + ", mode="
				+ getMode() + ", transactions=" + numTxs + ", operations="
				+ numOps;
		return s;
	}
}
