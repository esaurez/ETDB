package com.att.research.mdbc;

import java.sql.Connection;

public interface StateManager {

    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the connection, created by the server 
     * @return a new MdbcConnection that is associated with the corresponding connection Id
     */
    Connection GetConnection(String connectionId);
   
    /**
     * This function initialize the state of the MdbcServer
     * Operations to be performed:
     * 1) Prefetch the corresponding tables from Music
     */
    void InitializeSystem();
}
