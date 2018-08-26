package com.att.research.mdbc.configurations;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.DatabasePartition;
import com.att.research.mdbc.MDBCUtils;
import com.att.research.mdbc.Range;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NodeConfiguration {

    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(NodeConfiguration.class);

    public String sqlDatabaseName;
    public DatabasePartition partition;
    public String nodeName;

    public NodeConfiguration(String tables, String titIndex, String titTableName, String partitionId, String sqlDatabaseName, String node){
        partition = new DatabasePartition(toRanges(tables), titIndex, titTableName, partitionId, null) ;
        this.sqlDatabaseName = sqlDatabaseName;
        this.nodeName = node;
    }

    protected Set<Range> toRanges(String tables){
        Set<Range> newRange = new HashSet<>();
        String[] tablesArray=tables.split(",");
        for(String table: tablesArray) {
            newRange.add(new Range(table));
        }
        return newRange;
    }


    public void saveToFile(String file){
        try {
            String serialized = partition.toJson();
            MDBCUtils.saveToFile(serialized,file,LOG);
        } catch (IOException e) {
            e.printStackTrace();
            // Exit with error
            System.exit(1);
        }
    }
}
