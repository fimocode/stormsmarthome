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
    public Double total = Double.valueOf(0);

    public Bolt_avg(int windows) {
        this.windows = windows;
    }
    
    private OutputCollector _collector;
    
    public HashMap <Integer, HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > > > > map_house = new HashMap<>();

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer house_id     = (Integer) tuple.getValueByField("house_id");
        Double  value        = (Double) tuple.getValueByField("value");
        String household_deviceid = (String)tuple.getValueByField("household_deviceid");
        String year = (String)tuple.getValueByField("year");
        String month = (String)tuple.getValueByField("month");
        String date = (String)tuple.getValueByField("date");
        Long slice_num = (Long) tuple.getValueByField("slice_num");
        Double avg = (double) 0;
        if((Long)tuple.getValueByField("end")!=0){
            _collector.emit(new Values(house_id, household_deviceid, year, month, date, slice_num, Double.valueOf(0), (Long)tuple.getValueByField("end")));
            db_store.pushData(windows, map_house);
        }
        else{
            HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > > > house_data;
            HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > > device_data;
            HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > year_data;
            HashMap<String, HashMap<Long, HashMap<String, Double > > > month_data;
            HashMap<Long, HashMap<String, Double > > day_data;
            HashMap<String, Double > slice_data;
            house_data = map_house.getOrDefault(house_id, new HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > > >());
            device_data = house_data.getOrDefault(household_deviceid, new HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > >());
            year_data = device_data.getOrDefault(year, new HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > >());
            month_data = year_data.getOrDefault(month, new HashMap<String, HashMap<Long, HashMap<String, Double > > >());
            day_data = month_data.getOrDefault(date, new HashMap<Long, HashMap<String, Double > >());
            slice_data = day_data.getOrDefault(slice_num, new HashMap<String, Double >());
            slice_data.put("total", slice_data.getOrDefault("total", Double.valueOf(0))+value);
            slice_data.put("count", slice_data.getOrDefault("count", Double.valueOf(0))+1);
            if(slice_data.getOrDefault("count", Double.valueOf(0))!=0)
                avg = slice_data.get("total")/slice_data.get("count");
            slice_data.put("avg", avg);
            day_data.put(slice_num, slice_data);
            month_data.put(date, day_data);
            year_data.put(month, month_data);
            device_data.put(year, year_data);
            house_data.put(household_deviceid, device_data);
            map_house.put(house_id, house_data);
            System.out.printf("\ravg: %f",++total);
            _collector.emit(new Values(house_id, household_deviceid, year, month, date, slice_num, avg, (Long)tuple.getValueByField("end")));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","household_deviceid","year","month","date","slice_num","value","end"));
    }
    
}
