package com.att.research.mdbc.mixins;

public final class Operation {

	final OperationType TYPE;
	final String OLD_VAL;
	final String NEW_VAL;

	public Operation(OperationType type, String newVal, String oldVal) {
		TYPE = type;
		NEW_VAL = newVal;
		OLD_VAL = oldVal;
	}
}
