package com.att.research.mdbc;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;
import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.mixins.MusicMixin;
import com.att.research.mdbc.mixins.TxCommitProgress;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * \TODO Implement an interface for the server logic and a factory 
 * @author Enrique Saurez
 */
public class MdbcStateManager implements StateManager{

	//\TODO We need to fix the auto-commit mode and multiple transactions with the same connection

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MdbcStateManager.class);

	/**
	 * This is the interface used by all the MusicSqlManagers, 
	 * that are created by the MDBC Server 
	 * @see MusicInterface 
     */
    private MusicInterface musicManager;
    /**
     * This is the Running Queries information table.
     * It mainly contains information about the entities 
     * that have being committed so far.
     */
    private TxCommitProgress transactionInfo;
    
    private Map<String,MdbcConnection> mdbcConnections;
    
    private String url;
    
    private Properties info;
    
    public MdbcStateManager(String url, Properties info){
    	this.url = url;
    	this.info = info;
    	this.transactionInfo = new TxCommitProgress();
        String mixin  = info.getProperty(Configuration.KEY_MUSIC_MIXIN_NAME, Configuration.MUSIC_MIXIN_DEFAULT);
        this.musicManager = MixinFactory.createMusicInterface(mixin, url, info);
        this.musicManager.createKeyspace();
        this.musicManager.initializeMdbcDataStructures();
        MusicMixin.loadProperties();
        this.mdbcConnections = new HashMap<String,MdbcConnection>();
    }
  
    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the transaction, created using {@link CreateNewtransaction} 
     * @return
     */
    public Connection GetConnection(String id) {
    	if(mdbcConnections.containsKey(id)) {
    		//\TODO: Verify if this make sense
    		// Intent: reinitialize transaction progress, when it already completed the previous tx for the same connection
    		if(transactionInfo.isComplete(id)) {
    			transactionInfo.reinitializeTxProgress(id);
    		}
    		return mdbcConnections.get(id);
    	}

    	Connection sqlConnection, newConnection;
    	//Create connection to local SQL DB
		try {
			sqlConnection = DriverManager.getConnection(url, this.info);
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
			sqlConnection = null;
		}
		//Create MDBC connection
    	try {
			newConnection = new MdbcConnection(url, sqlConnection, info, this.musicManager);
		} catch (MDBCServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
			newConnection = null;
		}
		logger.info(EELFLoggerDelegate.applicationLogger,"Connection created for connection: "+id);

    	transactionInfo.createNewTransactionTracker(id, sqlConnection);
    	return newConnection;
    }
}
