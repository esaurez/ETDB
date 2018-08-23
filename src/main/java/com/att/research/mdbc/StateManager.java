package com.att.research.mdbc;

import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

public interface StateManager {

    /**
     * This function returns the connection to the corresponding transaction 
     * @param connectionId of the connection, created by the server
     * @return a new MdbcConnection that is associated with the corresponding connection Id
     */
    Connection GetConnection(String connectionId);
   
    /**
     * This function initialize the state of the MdbcServer
     * Operations to be performed:
     * 1) Prefetch the corresponding tables from Music
     */
    void InitializeSystem();

    void OpenConnection(String connectionId, Properties info);

    void CloseConnection(String connectionId);
}
