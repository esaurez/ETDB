package com.att.research.mdbc.mixins;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.att.research.logging.EELFLoggerDelegate;

import java.sql.Connection;


public class TxCommitProgress{
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(TxCommitProgress.class);

	private AtomicLong nextCommitId;
	private Map<String, CommitProgress> transactionInfo;

	public TxCommitProgress(){
		nextCommitId.set(0);
		transactionInfo = new ConcurrentHashMap<>();
	}
	
	public boolean containsTx(String txId) {
		return transactionInfo.containsKey(txId);
	}
	
	public long getNextCommitId() {
		return nextCommitId.getAndIncrement();
	}
	
	public void createNewTransactionTracker(String id, Connection conn) {
		transactionInfo.put(id, new CommitProgress(id,conn));
	}
	
	public void commitRequested(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing commit request",txId);
		}
		prog.setCommitRequested();
	}
	
	public void setSQLDone(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing saving completion of SQL",txId);
		}
		prog.setSQLCompleted();
	}

	public void setMusicDone(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing saving completion of Music",txId);
		}
		prog.setMusicCompleted();
	}
	
	public Connection getConnection(String txId){
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when retrieving statement",txId);
		}
		return prog.getConnection();
	}
	
	public void setRecordId(String txId, String partitionId, RedoRecordId recordId){
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when setting record Id",txId);
		}
		prog.setRecordId(partitionId,recordId);
	}
	
	public RedoRecordId getRecordId(String txId, String partitionId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when getting record Id",txId);
		}
		return prog.getRecordId(partitionId);
	}
	
	public boolean isRecordIdAssigned(String txId,String partitionId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when checking record",txId);
		}
		return prog.isRedoRecordAssigned(partitionId);
	}
	
	public boolean isComplete(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when checking completion",txId);
		}
		return prog.isComplete();
	}
	
	public void reinitializeTxProgress(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when reinitializing tx progress",txId);
		}
		prog.reinitialize();
	}
}

final class CommitProgress{
	String lTxId; // local transaction id  
	boolean commitRequested; //indicates if the user tried to commit the request already.
	boolean SQLDone; // indicates if SQL was already committed 
	boolean MusicDone; // indicates if music commit was already performed, atomic bool
	Connection connection;// reference to a connection object. This is used to complete a commit if it failed in the original thread.
	long timestamp; // last time this data structure was updated
	RedoRecordId redoRecordId;// record id for each partition

	public CommitProgress(String id,Connection conn){
		redoRecordIds=new HashMap<>();
		lTxId = id;
		commitRequested = false;
		SQLDone = false;
		MusicDone = false;
		connection = conn;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized boolean isComplete() {
		return commitRequested && SQLDone && MusicDone;
	}
	
	public synchronized void reinitialize() {
		redoRecordIds=new HashMap<>();
		commitRequested = false;
		SQLDone = false;
		MusicDone = false;
		timestamp = System.currentTimeMillis();
	}

	public synchronized void setCommitRequested() {
		commitRequested = true;
		timestamp = System.currentTimeMillis();
	}

	public synchronized void setSQLCompleted() {
		SQLDone = true;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized void setMusicCompleted() {
		MusicDone = true;
		timestamp = System.currentTimeMillis();
	}
	
	public Connection getConnection() {
		timestamp = System.currentTimeMillis();
		return connection;
	} 
	
	public long getTimestamInMillis() {
		return timestamp;
	}

	public synchronized void setRecordId(String partitionId, RedoRecordId id) {
		redoRecordIds.put(partitionId, id);
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized boolean isRedoRecordAssigned(String partitionId) {
		return redoRecordIds.containsKey(partitionId) && !redoRecordIds.get(partitionId).isEmpty();
	} 

	public synchronized RedoRecordId getRecordId(String partitionId) {
		return redoRecordIds.get(partitionId);
	} 

}
	