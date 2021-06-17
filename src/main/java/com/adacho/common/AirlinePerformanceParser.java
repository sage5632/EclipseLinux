package com.adacho.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
	private int year;
	private int month;
	private int day;

	private int arriveDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;

	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;

	private String uniqueCarrier;

	public AirlinePerformanceParser(Text text) {
		try {
			String[] columns = text.toString().split(",");
			
			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			day = Integer.parseInt(columns[2]);
			uniqueCarrier = columns[5];
			
			if(!columns[16].equals("")) {
				departureDelayTime = (int)Float.parseFloat(columns[16]);
			} else {
				departureDelayAvailable = false;
			}
			if(!columns[16].equals("")) {
				arriveDelayTime = (int)Float.parseFloat(columns[26]);
			} else {
				arriveDelayAvailable = false;
			}	
			if(!columns[16].equals("")) {
				distance = (int)Float.parseFloat(columns[37]);
			} else {
				distanceAvailable = false;
			}	
		} catch (Exception e) {
			// TODO: handle exception
		} 
			
	}



	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getDay() {
		return day;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
}