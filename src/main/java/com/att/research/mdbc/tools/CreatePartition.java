package com.att.research.mdbc.tools;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.DatabasePartition;
import com.att.research.mdbc.MDBCUtils;
import com.att.research.mdbc.Range;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CreatePartition {
    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(CreatePartition.class);

    @Parameter(names = { "-t", "--tables" }, required = true,
           description = "This is the tables that are assigned to this ")
   private String tables;
    @Parameter(names = { "-f", "--file" }, required = true,
            description = "This is the output file that is going to have the configuration for the ranges")
    private String file;
    @Parameter(names = { "-i", "--tit-index" }, required = true,
            description = "Index in the TiT Table")
    private String titIndex;
    @Parameter(names = { "-n", "--tit-table-name" }, required = true,
            description = "Tit Table name")
    private String titTable;
        @Parameter(names = { "-p", "--partition-id" }, required = true,
            description = "Partition Id")
    private String partitionId;
    @Parameter(names = { "-h", "-help", "--help" }, help = true,
            description = "Print the help message")
    private boolean help = false;

    private DatabasePartition partition;

    public CreatePartition(){
    }

    Set<Range> toRanges(String tables){
        Set<Range> newRange = new HashSet<>();
        String[] tablesArray=tables.split(",");
        for(String table: tablesArray) {
           newRange.add(new Range(table));
        }
        return newRange;
    }

    public void convert(){
        partition = new DatabasePartition(toRanges(tables), titIndex, titTable, partitionId, null) ;
    }

    public void saveToFile(){
        try {
            String serialized = partition.toJson();
            MDBCUtils.saveToFile(serialized,file,LOG);
        } catch (IOException e) {
            e.printStackTrace();
            // Exit with error
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        CreatePartition newPartition = new CreatePartition();
        @SuppressWarnings("deprecation")
        JCommander jc = new JCommander(newPartition, args);
        if (newPartition.help) {
            jc.usage();
            System.exit(1);
            return;
        }
        newPartition.convert();
        newPartition.saveToFile();
    }
}
