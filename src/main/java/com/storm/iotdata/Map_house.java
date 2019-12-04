package com.storm.iotdata;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Map_house
 */
public class Map_house implements Serializable {
    public volatile HashMap <Integer, HashMap<String, HashMap<Long, HashMap<String, Double > > > > map_house = new HashMap <Integer, HashMap<String, HashMap<Long, HashMap<String, Double > > > >();
}