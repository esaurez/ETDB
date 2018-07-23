package com.att.research.mdbc.mixins;

import java.io.Serializable;

import org.json.JSONObject;

public final class Operation implements Serializable{

	private static final long serialVersionUID = -1215301985078183104L;

	final OperationType TYPE;
	final JSONObject OLD_VAL;
	final JSONObject NEW_VAL;

	public Operation(OperationType type, JSONObject newVal, JSONObject oldVal) {
		TYPE = type;
		NEW_VAL = newVal;
		OLD_VAL = oldVal;
	}
}
