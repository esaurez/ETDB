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

import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.main.MusicCore;

import com.att.research.mdbc.TableInfo;

/**

 *
 */
public class MusicMixin implements MusicInterface {

	public static Map<Integer, Set<String>> currentLockMap = new HashMap<>();
	
	@Override
	public String getMixinName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMusicDefaultPrimaryKeyName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String generatePrimaryKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMusicKeyFromRow(TableInfo ti, String table, Object[] dbRow) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createKeyspace() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initializeMusicForTable(TableInfo ti, String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createDirtyRowTable(TableInfo ti, String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropDirtyRowTable(String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearMusicForTable(String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void markDirtyRow(TableInfo ti, String tableName, Object[] keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanDirtyRow(TableInfo ti, String tableName, Object[] keys) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Map<String, Object>> getDirtyRows(TableInfo ti, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteFromEntityTableInMusic(TableInfo ti, String tableName, Object[] oldRow) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDirtyRowAndEntityTableInMusic(TableInfo ti, String tableName, Object[] changedRow) {
		updateDirtyRowAndEntityTableInMusic(tableName, changedRow, false);
		
	}
	
	public void updateDirtyRowAndEntityTableInMusic(String tableName, Object[] changedRow, boolean isCritical) {
		// TODO Auto-generated method stub
		
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
	public String getPrimaryKey(TableInfo ti, String tableName, Object[] changedRow) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initializeMdbcDataStructures() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createMdbcDataStructures() {
		// TODO Auto-generated method stub
		
	}
}
