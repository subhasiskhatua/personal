package com.subhasis.personal.model;

import java.io.Serializable;
import java.util.List;

public class CustattDouble implements Serializable {

	private static final long serialVersionUID = -3140663456700722884L;
	
	private String key;
	private List<Long> value;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public List<Long> getValue() {
		return value;
	}
	public void setValue(List<Long> value) {
		this.value = value;
	}
}
