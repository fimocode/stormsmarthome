/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.File;
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
    public int windows;
    public int trigger_windows = 0;
    public float log_gap = 20; //percentage
    public boolean checkMin = false, checkAvg = true, checkMax = true;
    public Double total = Double.valueOf(0);
    public HashMap<String, DeviceData> data_list = new HashMap<String, DeviceData>();
    public HashMap<String, DeviceProp> data_prop_list = new HashMap<String, DeviceProp>();
    public Stack<DeviceNotification> noti_list = new Stack<DeviceNotification>();

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
        if(tuple.contains("trigger")){
            if(((++trigger_windows)%windows)==0){
                trigger_windows = 0;
                Long startTime = (Long) tuple.getValueByField("trigger");
                Long spoutSpeed = (Long) tuple.getValueByField("spout-speed");
                Long spoutLoad = (Long) tuple.getValueByField("spout-load");
                Long spoutTotal = (Long) tuple.getValueByField("spout-total");

                Stack<String> needClean = new Stack<String>();
                int newSave = 0;
                Stack<DeviceData> needSave = new Stack<DeviceData>();

                //Init data for save and clean procedure
                for(String key : data_list.keySet()){
                    DeviceData data = data_list.get(key);
                    if(!data.isSaved()){
                        needSave.push(data);
                    }
                    else if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate())>(60000*windows)){
                        needClean.push(key);
                    }
                }

                //DB store
                if(DB_store.pushDeviceData(needSave, new File("./tmp/device2db-" + windows + ".lck"))){
                    for(DeviceData data : needSave){
                        data_list.put(data.getUniqueID(), data.saved());
                        newSave++;
                    }
                }

                //Check over ceiling
                
                for(DeviceData data : needSave){
                    String data_prop_unique_id = String.format("%d_%d_%d", data.house_id, data.household_id, data.device_id);
                    DeviceProp data_prop = data_prop_list.getOrDefault(data_prop_unique_id, new DeviceProp(data.house_id, data.household_id, data.device_id, this.windows, data.getAvg(), data.getAvg(), data.getAvg()));
                    if(checkMax && data_prop.getMax()!=0 && (data.getAvg()-data_prop.getMax())>=(data_prop.getMax()*log_gap/100)){
                        //Check over Max
                        noti_list.push(new DeviceNotification(1, data, data_prop));
                    }
                    if(checkAvg && data_prop.getAvg()!=0 && (data.getAvg()-data_prop.getAvg())>=(data_prop.getAvg()*log_gap/100)){
                        //Check over Avg
                        noti_list.push(new DeviceNotification(0, data, data_prop));
                    }
                    if(checkMin && data_prop.getMin()!=0 && (data_prop.getMin()-data.getAvg())<=(data_prop.getMin()*log_gap/100)){
                        //Check under Min
                        noti_list.push(new DeviceNotification(-1, data, data_prop));
                    }
                    //Save data_prop
                    data_prop_list.put(data_prop_unique_id, data_prop.addValue(data.getAvg()));
                }

                System.out.printf("\n[Bolt_avg_%d] Noti list: %-15d\n", windows, noti_list.size());

                //Push Noti
                MQTT_Publisher.notificationsPublish(noti_list);
                //Save Noti
                if(DB_store.pushDeviceNotification(noti_list, new File("./tmp/devicenoti2db-" + windows + ".lck"))){
                    //Noti saved
                    System.out.printf("\n[Bolt_avg_%d] Saved to DB %-15d notifications\n", windows, noti_list.size());
                }

                //Clean unchanged data
                for(String key : needClean){
                    data_list.remove(key);
                }
                System.out.printf("\n[Bolt_avg_%d] Total: %-15d | Already saved: %-15d | Need save: %-15d | Saved: %-15d | Need clean: %-15d\n",windows, data_list.size(), data_list.size()-needClean.size()-needSave.size(), needSave.size(), newSave, needClean.size());
            }
        }
        else{
            Integer house_id        = (Integer) tuple.getValueByField("house_id");
            Integer household_id    = (Integer)tuple.getValueByField("household_id");
            Integer device_id       = (Integer)tuple.getValueByField("device_id");
            String year             = (String)tuple.getValueByField("year");
            String month            = (String)tuple.getValueByField("month");
            String day              = (String)tuple.getValueByField("day");
            Integer slice_num       = (Integer) tuple.getValueByField("slice_num");
            Double  value           = (Double) tuple.getValueByField("value");
            String unique_id = String.format("%d_%d_%d_%s_%s_%s_%d", house_id, household_id, device_id, year, month, day, slice_num);
            data_list.put(unique_id, data_list.getOrDefault(unique_id, new DeviceData(house_id, household_id, device_id, year, month, day, slice_num, windows)).increaseValue(value));
            _collector.emit(new Values(house_id, household_id, device_id, year, month, day, slice_num, data_list.get(unique_id).getAvg()));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","household_id","device_id","year","month","day","slice_num","avg"));
    }
    
}