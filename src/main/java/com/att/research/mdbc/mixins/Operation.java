package com.att.research.mdbc.mixins;

import java.io.Serializable;

public final class Operation implements Serializable{

	private static final long serialVersionUID = -1215301985078183104L;

	final OperationType TYPE;
	final String OLD_VAL;
	final String NEW_VAL;

	public Operation(OperationType type, String newVal, String oldVal) {
		TYPE = type;
		NEW_VAL = newVal;
		OLD_VAL = oldVal;
	}
}
