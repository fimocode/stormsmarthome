/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

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
    public int windows = 0;
    public Double total = Double.valueOf(0);
    public HashMap<String, DeviceData> data_list = new HashMap<String, DeviceData>();

    public Bolt_avg(int windows) {
        this.windows = windows;
    }
    
    private OutputCollector _collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer house_id        = (Integer) tuple.getValueByField("house_id");
        Integer household_id    = (Integer)tuple.getValueByField("household_id");
        Integer device_id       = (Integer)tuple.getValueByField("device_id");
        String year             = (String)tuple.getValueByField("year");
        String month            = (String)tuple.getValueByField("month");
        String day              = (String)tuple.getValueByField("day");
        Integer slice_num       = (Integer) tuple.getValueByField("slice_num");
        Double  value           = (Double) tuple.getValueByField("value");
        String unique_id = String.format("%d_%d_%d_%s_%s_%s_%d", house_id, household_id, device_id, year, month, day, slice_num);
        if((Long)tuple.getValueByField("end")!=0){
            Stack<String> needClean = new Stack<String>();
            int newSave = 0;
            Stack<DeviceData> needSave = new Stack<DeviceData>();
            _collector.emit(new Values(house_id, household_id, device_id, year, month, day, slice_num, value, (Long)tuple.getValueByField("end"))); //Trigger next bolt
            for(String key : data_list.keySet()){
                DeviceData data = data_list.get(key);
                if(!data.isSaved()){
                    needSave.push(data);
                }
                else if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate())>(120000*windows)){
                    needClean.push(key);
                }
            }
            for(DeviceData data : needSave){
                data_list.put(data.getUniqueID(), data.saved());
            }
            // for(String key : db_store.pushDeviceData(needSave)){
            //     data_list.put(key, data_list.get(key).saved());
            //     newSave++;
            // }
            for(String key : needClean){
                data_list.remove(key);
            }
            System.out.printf("\n\n[Bolt_avg_%-3d] Total: %-15d | Already saved: %-15d | Need save: %-15d | Saved: %-15d | Need clean: %-15d\n\n",windows, data_list.size(), data_list.size()-needClean.size()-needSave.size(), needSave.size(), newSave, needClean.size());
        }
        else{
            data_list.put(unique_id, data_list.getOrDefault(unique_id, new DeviceData(house_id, household_id, device_id, year, month, day, slice_num, windows)).increaseValue(value));
            _collector.emit(new Values(house_id, household_id, device_id, year, month, day, slice_num, data_list.get(unique_id).getAvg(), new Long(0)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","household_id","device_id","year","month","day","slice_num","avg","end"));
    }
    
}
