package com.att.research.mdbc;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.main.MusicCore;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DatabaseOperations {

    /**
     * This functions is used to generate cassandra uuid
     * @return a random UUID that can be used for fields of type uuid
     */
    public static String generateUniqueKey() {
		return UUID.randomUUID().toString();
	}

    /**
     * This functions returns the primary key used to managed a specific row in the TableToPartition tables in Music
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param tableName name of the application table that is being added to the system
     * @return primary key to be used with MUSIC
     */
    public static String getTableToPartitionPrimaryKey(String namespace, String tableToPartitionTableName, String tableName){
        return namespace+"."+tableToPartitionTableName+"."+tableName;
    }

    /**
     * Create a new row for a table, with not assigned partition
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param tableName name of the application table that is being added to the system
     * @param lockId if the lock for this key is already hold, this is the id of that lock. May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void createNewTableToPartitionRow(String namespace, String tableToPartitionTableName, String tableName,String lockId){
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,tableToPartitionTableName,tableName);
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(tableToPartitionTableName)
                .append(" (tablename) VALUES ")
                .append("('")
                .append(tableName)
                .append("');");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.criticalPut(namespace,tableToPartitionTableName,primaryKey,query,lockId,null);
    }

    /**
     * Update the partition to which a table belongs
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param table name of the application table that is being added to the system
     * @param newPartition partition to which the application table is assigned
     * @param lockId if the lock for this key is already hold, this is the id of that lock. May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void updatePartitionToTable(String namespace, String tableToPartitionTableName, String table, String newPartition, String lockId){
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,tableToPartitionTableName,table);
        PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder update = new StringBuilder("UPDATE ")
                .append(namespace)
                .append('.')
                .append(tableToPartitionTableName)
                .append(" SET previouspartitions = previouspartitions + ['")
                .append(newPartition)
                .append("'] WHERE tablename = '")
                .append(table)
                .append("';");
        query.appendQueryString(update.toString());
        MusicCore.criticalPut(namespace,tableToPartitionTableName,primaryKey,query,lockId,null);
    }


    public static String getPartitionInformationPrimaryKey(String namespace, String partitionInformationTable, String partition){
        return namespace+"."+partitionInformationTable+"."+partition;
    }

    /**
     * Create a new row, when a new partition is initialized
     * @param namespace namespace to which the partition info table resides in Cassandra
     * @param partitionInfoTableName name  of the partition information table
     * @param replicationFactor associated replicated factor for the partition (max of all the tables)
     * @param tables list of tables that are within this partitoin
     * @param lockId if the lock for this key is already hold, this is the id of that lock. May be <code>null</code> if lock is not hold for the corresponding key
     * @return the partition uuid associated to the new row
     */
    public static String createPartitionInfoRow(String namespace, String partitionInfoTableName, int replicationFactor, List<String> tables, String lockId){
        String id = generateUniqueKey();
        final String primaryKey = getPartitionInformationPrimaryKey(namespace,partitionInfoTableName,id);
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(partitionInfoTableName)
                .append(" (partition,replicationfactor,tables) VALUES ")
                .append("(")
                .append(id)
                .append(",")
                .append(replicationFactor)
                .append(",{");
        boolean first = true;
        for(String table: tables){
            if(!first){
                insert.append(",");
            }
            first = false;
            insert.append("'")
                    .append(table)
                    .append("'");
        }
        insert.append("'});");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.criticalPut(namespace,partitionInfoTableName,primaryKey,query,lockId,null);
        return id;
    }

    /**
     * Update the TIT row and table that currently handles the partition
     * @param namespace namespace to which the partition info table resides in Cassandra
     * @param partitionInfoTableName name  of the partition information table
     * @param partitionId row identifier for the partition being modiefd
     * @param newTitRow new TIT row and table that are handling this partition
     * @param owner owner that is handling the new tit row (url to the corresponding etdb nodej
     * @param lockId if the lock for this key is already hold, this is the id of that lock. May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void updateRedoRow(String namespace, String partitionInfoTableName, String partitionId, RedoRow newTitRow, String owner, String lockId){
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,partitionInfoTableName,partitionId);
        PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder update = new StringBuilder("UPDATE ")
                .append(namespace)
                .append('.')
                .append(partitionInfoTableName)
                .append(" SET currentowner='")
                .append(owner)
                .append("', latesttitindex=")
                .append(newTitRow.getRedoRowIndex())
                .append(", latesttittable='")
                .append(newTitRow.getRedoTableName())
                .append("' WHERE partition = ")
                .append(partitionId)
                .append(";");
        query.appendQueryString(update.toString());
        MusicCore.criticalPut(namespace,partitionInfoTableName,primaryKey,query,lockId,null);
    }

    /**
     * Create the first row in the history of the redo history table for a given partition
     * @param namespace namespace to which the redo history table resides in Cassandra
     * @param redoHistoryTableName name of the table where the row is being created
     * @param firstTitRow first tit  associated to the partition
     * @param partitionId partition for which a history is created
     */
	public static void createRedoHistoryBeginRow(String namespace, String redoHistoryTableName, RedoRow firstTitRow, String partitionId){
	    createRedoHistoryRow(namespace,redoHistoryTableName,firstTitRow,partitionId, new ArrayList<>());
    }

    /**
     * Create a new row on the history for a given partition
     * @param namespace namespace to which the redo history table resides in Cassandra
     * @param redoHistoryTableName name of the table where the row is being created
     * @param currentRow new tit row associated to the partition
     * @param partitionId partition for which a history is created
     * @param parentsRows parent tit rows associated to this partition
     */
	public static void createRedoHistoryRow(String namespace, String redoHistoryTableName, RedoRow currentRow, String partitionId, List<RedoRow> parentsRows){
	    final String primaryKey = partitionId+"-"+currentRow.getRedoTableName()+"-"+currentRow.getRedoRowIndex();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(redoHistoryTableName)
                .append(" (partition,redotable,redoindex,previousredo) VALUES ")
                .append("(")
                .append(partitionId)
                .append(",")
                .append(currentRow.getRedoTableName())
                .append(",")
                .append(currentRow.getRedoRowIndex())
                .append(",{");
        boolean first = true;
        for(RedoRow parent: parentsRows){
            if(!first){
                insert.append(",");
            }
            else{
                first = false;
            }
            insert.append("(")
                    .append(parent.getRedoTableName())
                    .append(",")
                    .append(parent.getRedoRowIndex())
                    .append("),");
        }
        insert.append("});");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.criticalPut(namespace,redoHistoryTableName,primaryKey,query,null,null);
    }

    /**
     * Creates a new empty tit row
     * @param namespace namespace where the tit table is located
     * @param titTableName name of the corresponding tit table where the new row is added
     * @param partitionId partition to which the redo log is hold
     * @return uuid associated to the new row
     */
    public static String CreateEmptyTitRow(String namespace, String titTableName, String partitionId){
        String id = generateUniqueKey();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(titTableName)
                .append(" (id,applied,latestapplied,partition,redo) VALUES ")
                .append("(")
                .append(id)
                .append(",false,-1,")
                .append(partitionId)
                .append(",[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.criticalPut(namespace,titTableName,id,query,null,null);
        return id;
    }
}
