package com.niallguerin.lsda.spark.weatherstation.app;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MainSparkWeatherStationApp {
	
	/*
	 * Main Spark Weather Station Application test class.
	 * 
	 * Set up list of weather stations, set up some temperatures,
	 * pass in single Double t for temp into the new method
	 * and check temps in range of t-1...t+1. This uses
	 * JavaRDD only. To keep code consise per guideline
	 * feedback from assignment 2 I added implements serializable
	 * on my WeatherStation and Measurement classes. Databricks
	 * documents that this is valid and normal for such cases
	 * to avoid serialization exception in Spark.
	 * 
	 * https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/javaionotserializableexception.html
	 * https://stackoverflow.com/questions/31105400/task-not-serializable-at-spark 
	 */
	public static void main(String[] args) {
		
		// set up galway and dublin weather station objects
		WeatherStation ws1 = new WeatherStation();
		WeatherStation ws2 = new WeatherStation();
		
		// do weatherstation1 - galway object setup
		ws1.setCity("galway");
		
		Measurement ws1m1 = new Measurement();
		ws1m1.setTemperature(20.0);
		ws1m1.setTime(1);
		
		Measurement ws1m2 = new Measurement();
		ws1m2.setTemperature(11.7);
		ws1m2.setTime(2);
		
		Measurement ws1m3 = new Measurement();
		ws1m3.setTemperature(-5.4);
		ws1m3.setTime(3);
		
		Measurement ws1m4 = new Measurement();
		ws1m4.setTemperature(18.7);
		ws1m4.setTime(4);
		
		Measurement ws1m5 = new Measurement();
		ws1m5.setTemperature(20.9);
		ws1m5.setTime(5);
		
		// add measurements to list for galway weather station
		List<Measurement> galwayMeasurementsList = new ArrayList<Measurement>();
		galwayMeasurementsList.add(ws1m1);
		galwayMeasurementsList.add(ws1m2);
		galwayMeasurementsList.add(ws1m3);
		galwayMeasurementsList.add(ws1m4);
		galwayMeasurementsList.add(ws1m5);
		
		// set measurement list on ws1
		ws1.setMeasurementList(galwayMeasurementsList);
		
		// do weatherstation2 - dublin object setup and measurements from 1-3PM
		ws2.setCity("dublin");
		
		Measurement ws2m1 = new Measurement();
		ws2m1.setTemperature(8.4);
		ws2m1.setTime(1);
		
		Measurement ws2m2 = new Measurement();
		ws2m2.setTemperature(19.2);
		ws2m2.setTime(2);
		
		Measurement ws2m3 = new Measurement();
		ws2m3.setTemperature(7.2);
		ws2m3.setTime(3);
		
		// add measurements to list for dublin weather station
		List<Measurement> dublinMeasurementsList = new ArrayList<Measurement>();
		dublinMeasurementsList.add(ws2m1);
		dublinMeasurementsList.add(ws2m2);
		dublinMeasurementsList.add(ws2m3);
		
		// set measurement list on ws2
		ws2.setMeasurementList(dublinMeasurementsList);
		
		// create the weather stations list
		List<WeatherStation> stations = new ArrayList<WeatherStation>();
		stations.add(ws1);
		stations.add(ws2);
		WeatherStation.setStations(stations);
		System.out.println("The weather stations list is: " + WeatherStation.getStations());
		
		/*
		 * I have created spark context here in the main driver program as normally I would not initialize
		 * apache spark inside a method like countTemperature(Double t). 
		 * 
		 * In that case, countTemperature(t) needs the sc passed
		 * as parameter to the method so that I don't create multiple spark contexts as I want to close
		 * all resources from main driver program and not the method. 
		 * 
		 * I created a package that has the alternative original version 
		 * which only has countTemperature(t) as well with single argument of temp being passed in and sc
		 * initialization and resource closing is all done within the method. I needed extra local variable
		 * in method in that case for returning the count as I cannot call return tempRDD.count once spark
		 * resources are closed which has to be done before the return statement to avoid leaks even if the
		 * compiler permits it.
		 * 
		 */
		
		// initialize spark
		System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
		SparkConf sparkConf = new SparkConf()
				.setAppName("WeatherStationTempCount")
				.setMaster("local[4]")
				.set("spark.executor.memory", "1g")
				.set("spark.driver.host", "localhost");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		/*
		 * Test Case 001: input = 20.0
		 * Expected Result: 3
		 */
		Double tc001TempCountInput = 20.0;
		
		/*
		 * Test Case 002: input = 8.0
		 * Expected Result: 2
		 */
		Double tc002TempCountInput = 8.0;

		// Display Results
		System.out.println("Test Case 001: Input = 20.0: Expected Result: 3. Actual Test Case Result: " + WeatherStation.countTemperature(tc001TempCountInput, sc) );
		System.out.println("Test Case 002: Input = 8.0: Expected Result: 2. Actual Test Case Result: " + WeatherStation.countTemperature(tc002TempCountInput, sc) );
		
		// clean up spark resources to avoid leaks
		sc.stop();
		sc.close();	
	}
}