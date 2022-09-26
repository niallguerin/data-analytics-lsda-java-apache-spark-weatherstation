package com.niallguerin.lsda.spark.weatherstation.app;

import java.io.Serializable;

public class Measurement implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private Integer time;
	private double temperature;
	
	public Integer getTime() {
		return time;
	}
	public void setTime(Integer time) {
		this.time = time;
	}
	public double getTemperature() {
		return temperature;
	}
	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}
	
	@Override
	public String toString() {
		return "Measurement [time=" + time + ", temperature=" + temperature + "]";
	}
	
}
