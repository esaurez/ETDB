package com.att.research.mdbc;

import java.sql.Connection;

public interface StateManager {

    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the connection, created by the server 
     * @return
     */
    Connection GetConnection(String connectionId);
}
