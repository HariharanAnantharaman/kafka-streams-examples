package com.shapira.examples.kstreamavg;

import java.io.Serializable;

import io.confluent.shaded.com.google.gson.Gson;
import io.confluent.shaded.com.google.gson.annotations.SerializedName;


public class SensorData implements Serializable {
	

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		final io.confluent.shaded.com.google.gson.Gson gson=new Gson();
		System.out.println("In SensorData, timestamp:"+this.timestamp);
		System.out.println("In SensorData, value:"+this.value);
		return gson.toJson(this);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SerializedName("n")
	 private  String n;
	
	 @SerializedName("t")
	 private  long timestamp;
	
	 @SerializedName("v")
	 private  Double value;

	public String getN() {
		return n;
	}

	public void setN(String n) {
		this.n = n;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	
	

	

}
