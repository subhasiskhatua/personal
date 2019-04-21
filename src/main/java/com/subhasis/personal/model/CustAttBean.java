package com.subhasis.personal.model;

import java.io.Serializable;
import java.util.List;

public class CustAttBean implements Serializable {

	private static final long serialVersionUID = -8784098915241613872L;
	
	String id;
	List<CustattString> pipeline_custatt_string;
	List<CustattDouble> pipeline_custatt_double;
	public String getId() {
		return id;
	}
	public List<CustattString> getPipeline_custatt_string() {
		return pipeline_custatt_string;
	}
	public List<CustattDouble> getPipeline_custatt_double() {
		return pipeline_custatt_double;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setPipeline_custatt_string(List<CustattString> pipeline_custatt_string) {
		this.pipeline_custatt_string = pipeline_custatt_string;
	}
	public void setPipeline_custatt_double(List<CustattDouble> pipeline_custatt_double) {
		this.pipeline_custatt_double = pipeline_custatt_double;
	}
}
