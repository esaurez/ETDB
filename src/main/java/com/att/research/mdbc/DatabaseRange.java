package com.att.research.mdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DatabaseRange {

	/**
	 * This subclass represent a range in Cassandra
	 * The range is composed of three sections:
	 * 1) Partition with Id lowerPartitionIndex and all rows with clusterIndex higher than lowerClusterIndex (inclusive)
	 * 2) All rows in partition betwee lowerPartitionIndex and upperPartitionIndex (non-inclusive)
	 * 3) Partition with Id upperPartitionIndex and all rows with clusterIndex lower than lowerClusterIndex (exclusive)
	 * @author Enrique Saurez 
	 */
	public class Range {
		final public String lowerPartitionIndex;//Comma-separated index
		final public String lowerClusterIndex;//Comma-separated index
		final public String upperPartitionIndex;//Comma-separated index
		final public String upperClusterIndex;//Comma-separated index
		public Range(String lowerPartition, String lowerCluster, String upperPartition, String upperCluster) {
			this.lowerPartitionIndex = lowerPartition;
			this.lowerClusterIndex = lowerCluster;
			this.upperPartitionIndex = upperPartition;
			this.upperClusterIndex = upperCluster;
		}
	
	}
	
	/**
	 * A range is composed of set of tables, and each tables can have a list of not necesarilly contiguos ranges.
	 * The only requirement is that the ranges are not overlapping.
	 * \TODO Verify that ranges are not overlapping
	 */
	protected Map<String,List<Range>> ranges;
	
	public DatabaseRange(Map<String,List<Range>> knownRanges) {
		if(knownRanges != null) {
			ranges = knownRanges;
		}
		else {
			ranges = new HashMap<String,List<Range>>();
		}
	}
	
	public void addNewRange(String tableName, Range newRange) {
		if(!ranges.containsKey(tableName)) {
			ranges.put(tableName, new ArrayList<Range>());
		}
		ranges.get(tableName).add(newRange);
		//\TODO Add verification that there is no overlap in the range
	}
	
	public Set<Entry<String,List<Range>>> getSnapshot() {
		return ranges.entrySet();
	}
}
