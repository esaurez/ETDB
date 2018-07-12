package com.att.research.mdbc;

import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.mixins.MusicMixin;
import com.att.research.mdbc.mixins.TxCommitProgress;

import java.sql.Connection;
import java.util.Properties;

/**
 * \TODO Implement an interface for the server logic and a factory 
 * @author enrique
 */
public class MdbcStateManager implements StateManager{

	//\TODO We need to fix the autocommit mode and multiple transactions with the same connection
	
	/**
	 * This is the interface used by all the MusicSqlManagers, 
	 * that are created by the Mdbc Server 
	 * @see MusicInterface 
     */
    private MusicInterface musicManager;
    /**
     * This is the Running Queries information table.
     * It mainly contains information about the entities 
     * that have being committed so far.
     */
    private TxCommitProgress transactionInfo;
    
    private Map<String,MdbcConnection> connections;
    
    public MdbcStateManager(String url, Properties info){
    	this.transactionInfo = new TxCommitProgress();
        String mixin  = info.getProperty(Configuration.KEY_MUSIC_MIXIN_NAME, Configuration.MUSIC_MIXIN_DEFAULT);
        this.musicManager = MixinFactory.createMusicInterface(mixin, url, info);
        this.musicManager.createKeyspace();
        this.musicManager.initializeMdbcDataStructures();
        MusicMixin.loadProperties();
        this.connections = new HashMap<String,MdbcConection>();
    }
  
    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the transaction, created using {@link CreateNewtransaction} 
     * @return
     */
    public Connection GetConnection(String id) {
    	if(transactionInfo.containsTx(id)) {
    		//\TODO: Verify if this make sense
    		// Intent: reinitialize tx progress, when it already completed the previous tx for the same connection
    		if(transactionInfo.isComplete(id)) {
    			transactionInfo.reinitializeTxProgress(id);
    		}
    		return transactionInfo.getConnection(id);
    	}

    	throw new UnsupportedOperationException("CreateNewTransaction needs to be implemented");
    }
}
