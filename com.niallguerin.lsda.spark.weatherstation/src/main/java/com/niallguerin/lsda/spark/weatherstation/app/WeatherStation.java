package com.niallguerin.lsda.spark.weatherstation.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * Assignment 3: Question 1.
 * 
 * Revised version of code from assignment 2 feedback on class abstraction.
 * 
 */

public class WeatherStation implements Serializable {
	
	private static final long serialVersionUID = 1L;
	static List<WeatherStation> stations = new ArrayList<WeatherStation>();
	private String city;
	private List<Measurement> measurementList = new ArrayList<Measurement>();

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public List<Measurement> getMeasurementList() {
		return measurementList;
	}

	public void setMeasurementList(List<Measurement> measurementList) {
		this.measurementList = measurementList;
	}

	public static List<WeatherStation> getStations() {
		return stations;
	}

	public static void setStations(List<WeatherStation> stations) {
		WeatherStation.stations = stations;
	}

	/*
	 * Based on assignment 2 feedback to simplify the method.
	 */
	public double getMaxTempOptimized(Integer startTime, Integer endTime) {
		List<Measurement> wsMeasurementList = this.getMeasurementList();

		return wsMeasurementList.stream().filter(m -> m.getTime() >= startTime && m.getTime() <= endTime)
				.mapToDouble(m -> m.getTemperature()).max().getAsDouble();
	}

	/*
	 * 
	 * Apache-Spark-driven method for Assignment 3: Q1.
	 * 
	 * Approximation rule: t-1, t+1 for temperature range across
	 * all weather stations.
	 * 
	 */
	public static long countTemperature(Double t, JavaSparkContext sc) 
	{	
		
		/*
		 * Parse the required temperatures from RDD - flatten the structure so we zoom in on the
		 * temperatures only from all weather stations, then filter out ones within the t-1...t+1 range.
		 * 
		 * Workflow:
		 * 1. Flatten the original weatherstations list using flatmap: get just temps from all weather stations.
		 * 2. Map from the resultset so we end up with just the list of temperature doubles.
		 * 3. Apply a filter with lambda expression to extract only temperatures in t-1...t+1 range for all temps.
		 */
		JavaRDD<Double> tempRDD = sc.parallelize( WeatherStation.stations )
				.flatMap( mlist -> mlist.getMeasurementList().iterator() )
				.map( theTemp -> theTemp.getTemperature() )
				.filter( theTempRange -> theTempRange >= t-1 && theTempRange <= t+1 );

		return 	tempRDD.count(); 
	}

}