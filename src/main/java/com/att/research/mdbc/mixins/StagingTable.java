package com.att.research.mdbc.mixins;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import com.att.research.logging.EELFLoggerDelegate;

public class StagingTable{
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(StagingTable.class);
	private HashMap<String,Deque<Operation>> operations;
	
	public StagingTable() {
		operations = new HashMap<>();
	}
	
	synchronized public void addOperation(String key, OperationType type, String oldVal, String newVal) {
		if(!operations.containsKey(key)) {
			operations.put(key, new LinkedList<Operation>());
		}
		operations.get(key).add(new Operation(type,newVal,oldVal));
	}
	
	synchronized public String compress() {
		//TODO use protobuf
		String compressed="";  
		return compressed;
	}
	
	synchronized public void decompress() {
		//TODO use protobuf
	}
	
	synchronized public Deque<Pair<String,Operation>> getIterableSnapshot() throws NoSuchFieldException{
		Deque<Pair<String,Operation>> response=new LinkedList<Pair<String,Operation>>();
		//\TODO: check if we can just return the last change to a given key 
		Set<String> keys = operations.keySet();
		for(String key : keys) {
			Deque<Operation> ops = operations.get(key);
			if(ops.isEmpty()) {
				logger.error(EELFLoggerDelegate.errorLogger, "Invalid state of the Operation data structure when creating snapshot");
				throw new NoSuchFieldException("Invalid state of the operation data structure");
			}
			response.add(Pair.of(key,ops.getLast()));
		}
		return response;
	}
}
