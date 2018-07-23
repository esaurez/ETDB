package com.att.research.mdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.TypedValue;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;

public class MdbcServerLogic extends JdbcMeta{

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MdbcServerLogic.class);

	StateManager manager;
	DatabasePartition ranges;

	public MdbcServerLogic(String Url, Properties info,DatabasePartition ranges) throws SQLException {
		super(Url,info);
		this.manager = new MdbcStateManager(Url,info,ranges);
		this.ranges = ranges;
	}
	
	@Override
	protected Connection getConnection(String id) throws SQLException {
		return this.manager.GetConnection(id);
	}

	
	//\TODO All the following functions can be deleted
	// Added for two reasons: debugging and logging
	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		StatementHandle h;
		try {
			h = super.prepare(ch, sql, maxRowCount);
			logger.info("prepared statement {}", h);
		} catch (Exception e ) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(e);
		}
		return h;
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame,
			PrepareCallback callback) throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.prepareAndExecute(h, sql, maxRowCount,maxRowsInFirstFrame,callback);
			logger.info("prepare and execute statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands)
			throws NoSuchStatementException {
		ExecuteBatchResult e;
		try {
			e = super.prepareAndExecuteBatch(h, sqlCommands);
			logger.info("prepare and execute batch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues)
			throws NoSuchStatementException {
		ExecuteBatchResult e;
		try {
			e = super.executeBatch(h, parameterValues);
			logger.info("execute batch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
			throws NoSuchStatementException, MissingResultsException {
		Frame f;
		try {
			f = super.fetch(h, offset, fetchMaxRowCount);
			logger.info("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return f;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount)
			throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.execute(h, parameterValues, maxRowCount);
			logger.info("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
			throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.execute(h, parameterValues, maxRowsInFirstFrame);
			logger.info("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		StatementHandle h;
		try {
			h = super.createStatement(ch);
			logger.info("create statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return h;
	}

	@Override
	public void closeStatement(StatementHandle h) {
		try {
			super.closeStatement(h);
			logger.info("statement closed {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}		
	}

	@Override
	public void openConnection(ConnectionHandle ch, Map<String, String> info) {
		try {
			super.openConnection(ch,info);
			logger.info("connection created with id {}", ch.id);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}		
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		try {
			super.closeConnection(ch);
			logger.info("connection closed with id {}", ch.id);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}		
	}

	@Override
	public void commit(ConnectionHandle ch) {
		try {
			super.closeConnection(ch);
			logger.info("connection commited with id {}", ch.id);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}				
	}

	@Override
	public void rollback(ConnectionHandle ch) {
		try {
			super.rollback(ch);
			logger.info("connection rollback with id {}", ch.id);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}				
	}
}

