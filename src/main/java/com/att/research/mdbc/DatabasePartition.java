package com.att.research.mdbc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.att.research.logging.EELFLoggerDelegate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A database range contain information about what ranges should be hosted in the current MDBC instance
 * A database range with an empty map, is supposed to contain all the tables in Music.  
 * @author Enrique Saurez 
 */
public class DatabasePartition {
	private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Driver.class);

	/**
	 * This subclass represent a range in Cassandra
	 * The range is composed of three sections:
	 * 1) Partition with Id lowerPartitionIndex and all rows with clusterIndex higher than lowerClusterIndex (inclusive)
	 * 2) All rows in partition between lowerPartitionIndex and upperPartitionIndex (non-inclusive)
	 * 3) Partition with Id upperPartitionIndex and all rows with clusterIndex lower than lowerClusterIndex (exclusive)
	 * @author Enrique Saurez 
	 */
	public class Range {
		final public String lowerPartitionIndex;//Comma-separated index
		final public String lowerClusterIndex;//Comma-separated index
		final public String upperPartitionIndex;//Comma-separated index
		final public String upperClusterIndex;//Comma-separated index
		final public String partitionId;
		private String transactionInformationIndex;//Index that can be obtained either from  
		private String lockId;//Index that can be obtained either from  

		public Range(String partitionId, String lowerPartition, String lowerCluster, String upperPartition, String upperCluster, String index, String LockId) {
			this.partitionId = partitionId;
			this.lowerPartitionIndex = lowerPartition;
			this.lowerClusterIndex = lowerCluster;
			this.upperPartitionIndex = upperPartition;
			this.upperClusterIndex = upperCluster;
			if(index != null) {
				this.setTransactionInformationIndex(index);
			}
			else {
				this.setTransactionInformationIndex("");
			}
			this.setLockId(lockId);
		}

		/**
		 * Compares to Range types
		 * @param other the other range against which this is copied
		 * @return the equality result
		 */
		public boolean equal(Range other) {
			return (lowerPartitionIndex == other.lowerPartitionIndex) &&
					(lowerClusterIndex == other.lowerClusterIndex) &&
					(upperPartitionIndex == other.upperPartitionIndex) &&
					(upperClusterIndex == other.upperClusterIndex);
		}

		public String getTransactionInformationIndex() {
			return transactionInformationIndex;
		}

		public void setTransactionInformationIndex(String transactionInformationIndex) {
			this.transactionInformationIndex = transactionInformationIndex;
		}

		public String getLockId() {
			return lockId;
		}

		public void setLockId(String lockId) {
			this.lockId = lockId;
		}
	}
	
	/**
	 * A range is composed of set of tables, and each tables can have a list of not necessarily contiguous ranges.
	 * The only requirement is that the ranges are not overlapping.
	 * \TODO Verify that ranges are not overlapping
	 * \NOTE we may want to change the list to a custom data structure that supports an operation similar to lower_bound and 
	 * upper_bound in C++, that we can use to find directly the range given a key value. 
	 */
	protected Map<String,List<Range>> ranges;
	
	public DatabasePartition() {
		ranges = new HashMap<String,List<Range>>();
	}
	
	public DatabasePartition(Map<String,List<Range>> knownRanges) {
		if(knownRanges != null) {
			ranges = knownRanges;
		}
		else {
			ranges = new HashMap<String,List<Range>>();
		}
	}
	
	/**
	 * Add a new range to the ones own by the database
	 * @param tableName name of the table that contains the range
	 * @param newRange range that is being added
	 */
	public synchronized void addNewRange(String tableName, Range newRange) {
		if(!ranges.containsKey(tableName)) {
			ranges.put(tableName, new ArrayList<Range>());
		}
		ranges.get(tableName).add(newRange);
		//\TODO Add verification that there is no overlap in the range
	}
	
	/**
	 * Delete a range that is being modified
	 * @param tableName name of the table that contains the range
	 * @param rangeToDel limits of the range
	 */
	public synchronized void deleteRange(String tableName, Range rangeToDel) {
		if(!ranges.containsKey(tableName)) {
			logger.error(EELFLoggerDelegate.errorLogger,"Table doesn't exist: "+tableName);
			throw new IllegalArgumentException("Invalid table"); 
		}
		int indexToDel = -1;
		int index = 0;
		for(Range range: ranges.get(tableName)) {
			if(rangeToDel.equal(range)) {
				indexToDel = index;
				break;
			}
			index++;
		}
		if(index == -1) {
			logger.error(EELFLoggerDelegate.errorLogger,"Range doesn't exist in table: "+tableName);
			throw new IllegalArgumentException("Invalid range"); 
		}
		ranges.get(tableName).remove(indexToDel);
	}
	
	/**
	 * Get all the ranges that are currently owned
	 * @return ranges
	 */
	public synchronized Set<Entry<String,List<Range>>> getSnapshot() {
		return ranges.entrySet();
	}
	
	/**
	 * Serialize the ranges
	 * @return serialized ranges
	 */
    public String toJson() {
    	GsonBuilder builder = new GsonBuilder();
    	builder.setPrettyPrinting().serializeNulls();;
        Gson gson = builder.create();
        return gson.toJson(this);	
    }
    
    /**
     * Function to obtain the configuration 
     * @param filepath path to the database range
     * @return a new object of type DatabaseRange
     * @throws FileNotFoundException
     */
    
    public static DatabasePartition readJsonFromFile( String filepath) throws FileNotFoundException {
    	 BufferedReader br;
		try {
			br = new BufferedReader(  
			         new FileReader(filepath));
		} catch (FileNotFoundException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
			throw e;
		}
    	Gson gson = new Gson();
    	DatabasePartition range = gson.fromJson(br, DatabasePartition.class);	
    	return range;
    }
}
