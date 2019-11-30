/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author kulz0
 */
class Bolt_avg extends BaseRichBolt {
    private int windows = 0;
    
    public Bolt_avg(int i, HashMap <Integer, HashMap<String, HashMap<Long, HashMap<Long, Double > > > > map_house) {
        this.windows = i;
        this.map_house = map_house;
    }
    
    private OutputCollector _collector;
    
    public HashMap <Integer, HashMap<String, HashMap<Long, HashMap<Long, Double > > > > map_house;
    
//    public HashMap < Integer, HashMap <String, HashMap<String, Double> > > data;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer house_id     = (Integer) tuple.getValueByField("house_id");
        Double  value        = (Double) tuple.getValueByField("value");
        String household_deviceid = (String)tuple.getValueByField("household_deviceid");
        String date = (String)tuple.getValueByField("date");
        Long index = (Long) tuple.getValueByField("index");
        Long slice_num = (Long) tuple.getValueByField("slice_num");
        String slice_name = date + " " +  String.format("%02d", Math.floorDiv((slice_num*windows),60)) + ":" +  String.format("%02d", (slice_num*windows)%60) + "->" +  String.format("%02d", Math.floorDiv(((slice_num+1)*windows),60)) + ":" +  String.format("%02d", ((slice_num+1)*windows)%60) ;
        if((Boolean)tuple.getValueByField("end")){
            _collector.emit(new Values(house_id, household_deviceid, slice_name, new Double("0"), true));
        }
        else{
            Double val = new Double(String.valueOf(0));
            Double avg =  new Double(String.valueOf(0));

            //Store data sample
            HashMap<String, HashMap<Long, HashMap<Long, Double > > > house;
            HashMap<Long, HashMap<Long, Double > > device;
            HashMap<Long, Double> slice;

            house = map_house.getOrDefault(house_id, new HashMap<String, HashMap<Long, HashMap<Long, Double>>>());
            device = house.getOrDefault(household_deviceid, new HashMap<Long, HashMap<Long, Double>>());
            slice = device.getOrDefault(slice_num, new HashMap<Long, Double>());
            slice.put(index, value);
            device.put(slice_num, slice);
            house.put(household_deviceid, device);

            //Cal avg
            for (Long sample : slice.keySet()) {
                val += slice.get(sample);
            }
            avg = val/slice.size();

    //        HashMap<String, HashMap<String, Double>> data_house = data.getOrDefault(house_id, new HashMap<String, HashMap<String, Double>>());
    //        HashMap<String, Double> data_slice = data_house.getOrDefault(slice_name, new HashMap<String, Double>());
    //        data_slice.put(household_deviceid, avg);
    //        data_house.put(slice_name, data_slice);
    //        data.put(house_id, data_house);
            _collector.emit(new Values(house_id, household_deviceid, slice_name, avg, false));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","household_deviceid","slice_name","value","end"));
    }
    
}
