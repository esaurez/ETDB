package com.att.research.camusic;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.h2.tools.Server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
/**
* <h1>MUSIC SQL Manager</h1>
* Program that helps  take data written to a SQL database
* and seamlessly integrate it with MUSIC that maintains data in a no-sql data-store (Cassandra) and protects
* access to it with a distributed locking service (based on Zookeeper)
* 
* 
*
* @author  Bharath Balasubramanian
* @version 1.0
* @since   2014-03-31 
*/
public class MusicSqlManager {
	private static Session musicSession = null;
	private static Connection dbConnection=null;
	private static MusicConnector mCon = null;
	final static Logger logger = Logger.getLogger(MusicSqlManager.class);
	private static boolean isUpdateInProgress=false;
	private static boolean configLoaded = false;
	
	public MusicSqlManager(){
		if(configLoaded == false){
			logger.info("Starting music-sql-manager with db listener info...");	
			ConfigDetails.populate();
			configLoaded = true;
		}
	}
	
	public synchronized static void setIsUpdateInProgress(boolean value){
		isUpdateInProgress = value;
	}
	
	public synchronized static boolean getIsUpdateInProgress(){
		return isUpdateInProgress;
	}
	/**
	 * This function initializes both the database and MUSIC for the new table
	 * @param tableName This is the table on which triggers are being created.
	 */
	public void initializeDbAndMusicForTable(String tableName){
		createSQLTriggers(tableName);
		createMusicKeyspace();
		createDataAndDirtyMusicTables(tableName);	
	}
	
	/**
	 * This function create triggers on the database for each row after every insert
	 * update and delete and before every select.
	 * @param tableName This is the table on which triggers are being created.
	 */
	private void createSQLTriggers(String tableName){
		try {
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS TRI_INS_"+tableName+" AFTER INSERT ON "+tableName+"  FOR EACH ROW CALL \""+ConfigDetails.triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS TRI_UPDATE_"+tableName+" AFTER UPDATE ON "+tableName+"  FOR EACH ROW CALL \""+ConfigDetails.triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS TRI_DEL_"+tableName+" AFTER DELETE ON "+tableName+"  FOR EACH ROW CALL \""+ConfigDetails.triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS TRI_SEL_"+tableName+" BEFORE SELECT ON "+tableName+"  CALL \""+ConfigDetails.triggerClassName+"\"");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Music-related functions
	 */
	/**
	 * 
	 */
	/**
	 * This function creates a keyspace in Music/Cassandra to store the data 
	 * corresponding to the sql tables
	 */
	private void createMusicKeyspace(){
		String keyspaceCreate = "CREATE KEYSPACE IF NOT EXISTS camunda  "
				+ "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
		executeMusicWriteQuery(keyspaceCreate);	
	}
	
	/**
	 * This function creates two tables for every table in sql: (i) a table with the exact same name 
	 * as the sql table storing the sql data in text format. (ii) a dirty bits table that stores the keys in the cassandra table
	 * that are yet to be updated in the sql table (they were written by some other node)
	 * 
	 * @param tableName This is the table name of the sql table
	 */
	private void createDataAndDirtyMusicTables(String tableName){	
		ArrayList<String> columnNames = getSQLColumnNames(tableName);
		int numOfFields = columnNames.size();
		
		String fieldsString = "(";
		for(int counter =0; counter < numOfFields;++counter){		
			fieldsString = fieldsString +"f"+counter+" text";
			if(counter ==0)
				fieldsString = fieldsString +" PRIMARY KEY";
			if(counter==numOfFields-1)
				fieldsString = fieldsString+")";
			else 
				fieldsString = fieldsString+",";
		}
		
		
		String mainTableQuery =  "CREATE TABLE IF NOT EXISTS camunda."+tableName+" "+ fieldsString+";"; 
		executeMusicWriteQuery(mainTableQuery);
		
		
		//create dirtybitsTable at all replicas 
		for(int i=0; i < ConfigDetails.allReplicaIds.length;++i){
			String dirtyRowsTableName = "dirty_"+tableName+"_"+ConfigDetails.allReplicaIds[i];
			String dirtyTableQuery = "CREATE TABLE IF NOT EXISTS camunda."+ dirtyRowsTableName+" (dirtyRowKeys text PRIMARY KEY);"; 
			executeMusicWriteQuery(dirtyTableQuery);
		}

	}

	/**
	 * This function is called whenever there is an insert or update to a local sql table, wherein it updates the 
	 * music/cassandra tables (both dirty bits and actual data) corresponding to the sql write. Music propagates it to 
	 * the other replicas
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed
	 * @param changedRowKey This is the primary key of the changed row
	 */
	public  void updateDirtyRowAndEntityTableInMusic(String tableName,Object[] changedRow, String changedRowKey){
		//mark the dirty rows in music for all the replica tables
		for(int i=0; i < ConfigDetails.allReplicaIds.length;++i){
			String dirtyRowsTableName = "dirty_"+tableName+"_"+ConfigDetails.allReplicaIds[i];
			String dirtyTableQuery = "INSERT INTO camunda."+ dirtyRowsTableName+"(dirtyRowKeys) VALUES ($$'"+ changedRowKey+"'$$);"; 
			executeMusicWriteQuery(dirtyTableQuery);
		}
		
		//format the db row
		
		String valueString = "(";
		String fieldsString="(";	
		for(int i =0; i < changedRow.length;++i){
			Object entry = changedRow[i];
			fieldsString = fieldsString+"f"+i;
			if(entry == null)
				valueString = valueString + "'null'"; 
			else if((entry instanceof String) || (entry instanceof Timestamp))
					valueString = valueString + "$$'"+entry+"'$$";
			else
				valueString = valueString +"'"+entry+"'";
			if(i == (changedRow.length -1)){
				fieldsString = fieldsString+")";
				valueString = valueString+")";
			}
			else{
				fieldsString = fieldsString+",";
				valueString = valueString+",";
			}
		}

		//update local music node. note: in cassandra u can insert again on an existing key..it becomes and update
		String musicQuery =  "INSERT INTO camunda."+tableName+" "+ fieldsString+" VALUES "+ valueString+";";   
		executeMusicWriteQuery(musicQuery);
	}
	
	/**
	 * This function is called whenever there is a delete to a row on  a local sql table, wherein it updates the 
	 * music/cassandra tables (both dirty bits and actual data) corresponding to the sql write. Music propagates it to 
	 * the other replicas
	 * @param tableName This is the table that has changed.
	 * @param deletedRowKey This is the primary key of the delete row
	 */
	public  void deleteFromEntityTableInMusic(String tableName,String deletedRowKey){
		String musicQuery =  "DELETE FROM camunda."+tableName+"  WHERE f0=$$'"+ deletedRowKey+"'$$;";   
		executeMusicWriteQuery(musicQuery);
	}
	
	/**
	 * This function is called whenever there is a select on a local sql table, wherein it first checks the local
	 * dirty bits table to see if there are any keys in cassandra whose value has not yet been sent to sql
	 * @param tableName This is the table on which the select is being performed
	 */
	public  void readDirtyRowsAndUpdateDb(String tableName){
		//read dirty rows of this table from Music
		String dirtyRowIdsTableName = "camunda.dirty_"+tableName+"_"+ConfigDetails.myId;
		String dirtyRowIdsQuery = "select * from "+dirtyRowIdsTableName+";";
		ResultSet results = executeMusicRead(dirtyRowIdsQuery);
		for (com.datastax.driver.core.Row row : results) {
			logger.info("-----------------------");
			String dirtyRowId = row.getString(0);//only one column
			String dirtyRowQuery = "select * from camunda."+tableName+" where f0=$$"+dirtyRowId+"$$;";
			ResultSet dirtyRows = executeMusicRead(dirtyRowQuery);
			for (com.datastax.driver.core.Row singleDirtyRow : dirtyRows){//there can only be one. 
				writeMusicRowToSQLDb(singleDirtyRow,tableName,ConfigDetails.primaryKeyName,getSQLColumnNames(tableName));
			}
		}
	}
	
	/**
	 * This functions copies the contents of a row in Music into the corresponding row in the sql table
	 * @param musicRow This is the row in Music that is being copied into sql
	 * @param tableName This is the name of the table in both Music and swl
	 * @param primaryKeyName This is the primary key of the row
	 * @param columnNames These are the column names
	 */
	private void writeMusicRowToSQLDb(com.datastax.driver.core.Row musicRow, String tableName, String primaryKeyName,ArrayList<String> columnNames){
		//set the update in progress flag to ensure that we dont cyclically update again
		setIsUpdateInProgress(true);

		//first construct the value string and column name string for the db write
		int numOfColumns = columnNames.size();
		String valueString = "(";
		String columnNameString = "("; //needed onyl when we are doing an update
		for(int i=0; i < numOfColumns ;++i){
			valueString = valueString + musicRow.getString("f"+i);
			columnNameString = columnNameString + columnNames.get(i);
			if(i == (numOfColumns -1)){
				valueString = valueString+")";
				columnNameString = columnNameString +")";
			}
			else{
				valueString = valueString+",";
				columnNameString = columnNameString +",";
			}
		}
		String primaryKeyValue = musicRow.getString("f0");
		String dbWriteQuery=null;
		try{
			dbWriteQuery = "INSERT INTO "+tableName+" VALUES"+valueString+";";
			executeSQLWrite(dbWriteQuery);
		} catch (SQLException e) {
			logger.info("Insert failed because row exists, do an update");
			dbWriteQuery = "UPDATE "+tableName+" SET "+columnNameString+" = "+ valueString +"WHERE "+primaryKeyName+"="+primaryKeyValue+";";
			try {
				executeSQLWrite(dbWriteQuery);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}

/*		String selectQuery = "select "+ primaryKeyName+" FROM "+tableName+" WHERE "+primaryKeyName+"="+primaryKeyValue+";";
		java.sql.ResultSet rs = executeSQLRead(selectQuery);
		String dbWriteQuery=null;
		try {
			if(rs.next()){//this entry is there, do an update
				dbWriteQuery = "UPDATE "+tableName+" SET "+columnNameString+" = "+ valueString +"WHERE "+primaryKeyName+"="+primaryKeyValue+";";
			}else
				dbWriteQuery = "INSERT INTO "+tableName+" VALUES"+valueString+";";
			executeSQLWrite(dbWriteQuery);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		//clean the music dirty bits table
		String dirtyRowIdsTableName = "camunda.dirty_"+tableName+"_"+ConfigDetails.myId;
		String deleteQuery = "delete from "+dirtyRowIdsTableName+" where dirtyRowKeys=$$"+primaryKeyValue+"$$;";
		executeMusicWriteQuery(deleteQuery);
	}
	
	/**
	 * Clears the dirty bits table (for all replicas) and the main table in Music for this db table
	 * @param tableName This is the table that has been deleted
	 */
	public void clearMusicForTable(String tableName){
		executeMusicWriteQuery("drop table camunda."+tableName+";");
		for(int i=0; i < ConfigDetails.allReplicaIds.length;++i)
			executeMusicWriteQuery("drop table camunda.dirty_"+tableName+"_"+i+";");
	}
	
	/**
	 * This method executes a write query in Music
	 * @param query
	 */
	public  void executeMusicWriteQuery(String query){
		logger.info("Executing music write:"+ query);
		getMusicSession().execute(query);
	}
	
	/**
	 * This method executes a read query in Music
	 * @param query
	 */
	public  ResultSet executeMusicRead(String query){
		logger.info("Executing music read:"+ query);
		return getMusicSession().execute(query);
	}
	
	/**This method gets a connection to Music
	 * @return
	 */
	public Session getMusicSession(){	
		//create cassa session
		if(musicSession == null){
			logger.info("New Music session created");
			mCon = new MusicConnector(ConfigDetails.musicAddress);
			musicSession = mCon.getSession();
		}
		return musicSession;
	}

	
	/*
	 * SQL related functions
	 */
	
	/**
	 * This method queries the information_schema table to obtain the column names of a particular table
	 * @param tableName
	 * @return
	 */
	private ArrayList<String> getSQLColumnNames(String tableName){
		ArrayList<String> columnNames = new ArrayList<String>();
		try {
			java.sql.ResultSet rsColumnNames = executeSQLRead("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS C where TABLE_NAME='"+tableName+"'");
			while(rsColumnNames.next()){
				String columnName = rsColumnNames.getString("COLUMN_NAME");
				columnNames.add(columnName);			
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return columnNames;
	}
	/**
	 * This method executes a write query in the sql database. 
	 * @param query
	 */
	public  void executeSQLWrite(String query) throws SQLException{
			logger.info("Executing SQL write:"+ query);
			Connection con = getDbConnection();
			//con.setAutoCommit(false);
			Statement stmt = con.createStatement();
			stmt.execute(query);
			stmt.close();
		//	con.commit();
			logger.info("Executed SQL write:"+ query);
	}
	
	/**
	 * This method executes a read query in the sql database
	 * @param query
	 */
	public java.sql.ResultSet executeSQLRead(String query){
		logger.info("Executing SQL read:"+ query);
		java.sql.ResultSet rs = null;
		try {
			Connection con = getDbConnection();
			con.setAutoCommit(false);
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rs;
	}
	
	/**This method gets a connection to the sql database
	 * @return
	 */
    public Connection getDbConnection() {
    	if(dbConnection == null){
			logger.info("New db connection created");
	        try {
	            Class.forName(ConfigDetails.dbDriver);
	        } catch (ClassNotFoundException e) {
	        	logger.info(e.getMessage());
	        }
	        try {
	            dbConnection = DriverManager.getConnection(ConfigDetails.dbConnection, ConfigDetails.dbUser, ConfigDetails.dbPassword);
	            return dbConnection;
	        } catch (SQLException e) {
	        	logger.info(e.getMessage());
	        }
	    	}
        return dbConnection;
    }

    public void close(){
    	try {
			getDbConnection().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	mCon.close();
    }
    
	public static void main(String[] args) throws Exception {
		MusicSqlManager handle = new MusicSqlManager();
		java.sql.ResultSet dbRs;
/*		
    	handle.executeSQLWrite("CREATE TABLE TEST_NO_KEYS (id int, name varchar(255))");
    	

    	handle.executeSQLWrite("CREATE TABLE PERSON (ID_ varchar(255), name varchar(255), primary key (ID_))");
    	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('1', 'Anju')");
    	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('2', 'Sonia')");
    	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('3', 'Sameer')");
    	   	
    	handle.executeSQLWrite("UPDATE PERSON SET NAME='Sonia Sharma' where ID_='2'");
    	
    	handle.executeSQLWrite("DELETE FROM PERSON WHERE ID_='1'");


 		dbRs = handle.executeSQLRead("select * from PERSON");

		while (dbRs.next()) {
			logger.info("ID_ " + dbRs.getInt("ID_") + " Name " + dbRs.getString("name"));
		}

		handle.executeSQLWrite("DROP table PERSON");
		handle.executeSQLWrite("DROP table TEST_NO_KEYS");
		
*/

		
	//	handle.executeSQLWrite("CREATE TABLE PERSON_TEST (ID_ varchar(255), name varchar(255), primary key (ID_))");
//		handle.initializeDbAndMusicForTable("PERSON_TEST");
		while(true){
		//	dbRs = handle.executeSQLRead("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC' ");
			String publicTables="";
	//		while (dbRs.next()) {
		//		publicTables = publicTables +"|"+dbRs.getString("TABLE_NAME");
			//}
			logger.info("Public Tables:"+publicTables);
			Thread.sleep(2000);
		}
	}
}
