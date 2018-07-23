package com.att.research.mdbc.mixins;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.json.JSONObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.main.MusicCore;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.mdbc.DatabasePartition;
import com.att.research.mdbc.TableInfo;

/**

 *
 */
public class MusicMixin implements MusicInterface {

	public static Map<Integer, Set<String>> currentLockMap = new HashMap<>();
	
	@Override
	public String getMixinName() {
		// 
		return null;
	}

	@Override
	public String getMusicDefaultPrimaryKeyName() {
		// 
		return null;
	}

	@Override
	public String generateUniqueKey() {
		// 
		return null;
	}

	@Override
	public String getMusicKeyFromRow(TableInfo ti, String table, JSONObject dbRow) {
		// 
		return null;
	}

	@Override
	public void close() {
		// 
		
	}

	@Override
	public void createKeyspace() {
		// 
		
	}

	@Override
	public void initializeMusicForTable(TableInfo ti, String tableName) {
		// 
		
	}

	@Override
	public void createDirtyRowTable(TableInfo ti, String tableName) {
		// 
		
	}

	@Override
	public void dropDirtyRowTable(String tableName) {
		// 
		
	}

	@Override
	public void clearMusicForTable(String tableName) {
		// 
		
	}

	@Override
	public void markDirtyRow(TableInfo ti, String tableName,  JSONObject keys) {
		// 
		
	}

	@Override
	public void cleanDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
		// 
		
	}

	@Override
	public List<Map<String, Object>> getDirtyRows(TableInfo ti, String tableName) {
		// 
		return null;
	}

	@Override
	public void deleteFromEntityTableInMusic(TableInfo ti, String tableName, JSONObject oldRow) {
		// 
		
	}

	@Override
	public void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName) {
		// 
		
	}

	@Override
	public void updateDirtyRowAndEntityTableInMusic(TableInfo ti, String tableName, JSONObject changedRow) {
		updateDirtyRowAndEntityTableInMusic(tableName, changedRow, false);
		
	}
	
	public void updateDirtyRowAndEntityTableInMusic(String tableName, JSONObject changedRow, boolean isCritical) {
		// 
		
	}
	
public static List<String> criticalTables = new ArrayList<>();
	
	public static void loadProperties() {
	    Properties prop = new Properties();
	    InputStream input = null;
	    try {
	      input = MusicMixin.class.getClassLoader().getResourceAsStream("mdbc.properties");
	      prop.load(input);
	      String crTable = prop.getProperty("critical.tables");
	      String[] tableArr = crTable.split(",");
	      criticalTables = Arrays.asList(tableArr);
	      
	    }
	    catch (Exception ex) {
	      ex.printStackTrace();
	    }
	    finally {
	      if (input != null) {
	        try {
	          input.close();
	        } catch (IOException e) {
	          e.printStackTrace();
	        }
	      }
	    }
	  }
	
	public static void releaseZKLocks(Set<String> lockIds) {
		for(String lockId: lockIds) {
			System.out.println("Releasing lock: "+lockId);
			try {
				MusicCore.voluntaryReleaseLock(lockId);
				MusicCore.destroyLockRef(lockId);
			} catch (MusicLockingException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public String getMusicKeyFromRowWithoutPrimaryIndexes(TableInfo ti, String tableName, JSONObject changedRow) {
		// 
		return null;
	}

	@Override
	public void initializeMdbcDataStructures() {
		// 
		
	}

	@Override
	public void createMdbcDataStructures() {
		// 
		
	}

	@Override
	public Object[] getObjects(TableInfo ti, String tableName, JSONObject row) {
		return null;
	}

	@Override
	public void commitLog(DBInterface dbi, HashMap<String, StagingTable> transactionDigest, DatabasePartition.Range partition, String commitId)
			throws MDBCServiceException {
		// TODO Auto-generated method stub
		
	}
}
