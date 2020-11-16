/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
 * @author hiiamlala
 */
class Bolt_avg extends BaseRichBolt {
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    public Double total = Double.valueOf(0);
    public HashMap<String, DeviceData> data_list = new HashMap<String, DeviceData>();
    public HashMap<String, DeviceProp> data_prop_list = new HashMap<String, DeviceProp>();

    public Bolt_avg(Integer gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
    }
    
    private OutputCollector _collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.contains("trigger")){
            if(((++triggerCount)%gap)==0){
                triggerCount = 0;
                Long startTime = (Long) tuple.getValueByField("trigger");
                Long spoutSpeed = (Long) tuple.getValueByField("spoutSpeed");
                Long spoutLoad = (Long) tuple.getValueByField("spoutLoad");
                Long spoutTotal = (Long) tuple.getValueByField("spoutTotal");

                Long startExec = System.currentTimeMillis();
                Stack<String> needClean = new Stack<String>();
                Integer newSave = 0;
                Stack<DeviceData> needSave = new Stack<DeviceData>();
                Stack<DeviceNotification> noti_list = new Stack<DeviceNotification>();

                //Init data for save and clean procedure
                for(String key : data_list.keySet()){
                    DeviceData data = data_list.get(key);
                    if(!data.isSaved()){
                        _collector.emit(new Values(data.getHouseId(), data.getHouseholdId(), data.getDeviceId(), data.getYear(), data.getMonth(), data.getDay(), data.getIndex(), data.getAvg()));
                        needSave.push(data);
                    }
                    else if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate())>(60000*gap)){
                        needClean.push(key);
                    }
                }

                //DB store
                if(DB_store.pushDeviceData(needSave, new File("./tmp/device2db-" + gap + ".lck"))){
                    for(DeviceData data : needSave){
                        data_list.put(data.getUniqueID(), data.saved());
                        newSave++;
                    }
                }

                for(String key : needClean){
                    data_list.remove(key);
                }

                //Check outlier
                for(DeviceData data : needSave){
                    String data_prop_uniqueId = String.format("%d_%d_%d", data.houseId, data.householdId, data.deviceId);
                    DeviceProp data_prop = data_prop_list.getOrDefault(data_prop_uniqueId, new DeviceProp(data.houseId, data.householdId, data.deviceId, this.gap, data.getAvg(), data.getAvg(), data.getAvg()));
                    if(config.isDeviceCheckMax() && data_prop.getMax()!=0 && (data.getAvg()-data_prop.getMax())>=(data_prop.getMax()*config.getDeviceLogGap()/100)){
                        //Check over Max
                        noti_list.push(new DeviceNotification(1, data, data_prop));
                    }
                    if(config.isDeviceCheckAvg() && data_prop.getAvg()!=0 && (data.getAvg()-data_prop.getAvg())>=(data_prop.getAvg()*config.getDeviceLogGap()/100)){
                        //Check over Avg
                        noti_list.push(new DeviceNotification(0, data, data_prop));
                    }
                    if(config.isDeviceCheckMin() && data_prop.getMin()!=0 && (data_prop.getMin()-data.getAvg())<=(data_prop.getMin()*config.getDeviceLogGap()/100)){
                        //Check under Min
                        noti_list.push(new DeviceNotification(-1, data, data_prop));
                    }
                    //Save data_prop
                    data_prop_list.put(data_prop_uniqueId, data_prop.addValue(data.getAvg()));
                }

                //Push Noti
                MQTT_Publisher.notificationsPublish(noti_list);
                //Save Noti
                if(DB_store.pushDeviceNotification(noti_list, new File("./tmp/devicenoti2db-" + gap + ".lck"))){
                    //Noti saved
                    // System.out.printf("\n[Bolt_avg_%-3d] Saved to DB %-10d notifications\n", gap, noti_list.size());
                }

                Long execTime = System.currentTimeMillis() - startExec;
                System.out.printf("\n[Bolt_avg_%-3d] Noti list: %-10d\n", gap, noti_list.size());
                System.out.printf("\n[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Saved: %-10d | Need clean: %-10d\n",gap, data_list.size(), data_list.size()-needSave.size(), needSave.size(), newSave, needClean.size());
                System.out.printf("\n[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000);

                try {
                    FileWriter log = new FileWriter(new File("tmp/bolt_avg_"+ gap +".tmp"), false);
                    PrintWriter pwOb = new PrintWriter(log , false);
                    pwOb.flush();
                    log.write(String.format("[Bolt_avg_%-3d] Noti list: %-10d\n", gap, noti_list.size()));
                    log.write(String.format("[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Saved: %-10d | Need clean: %-10d\n",gap, data_list.size(), data_list.size()-needSave.size(), needSave.size(), newSave, needClean.size()));
                    log.write(String.format("[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                    pwOb.close();
                    log.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
            }
        }
        else{
            Integer houseId        = (Integer) tuple.getValueByField("houseId");
            Integer householdId    = (Integer)tuple.getValueByField("householdId");
            Integer deviceId       = (Integer)tuple.getValueByField("deviceId");
            String year             = (String)tuple.getValueByField("year");
            String month            = (String)tuple.getValueByField("month");
            String day              = (String)tuple.getValueByField("day");
            Integer index       = (Integer) tuple.getValueByField("index");
            Double  value           = (Double) tuple.getValueByField("value");
            String uniqueId = String.format("%d_%d_%d_%s_%s_%s_%d", houseId, householdId, deviceId, year, month, day, index);
            data_list.put(uniqueId, data_list.getOrDefault(uniqueId, new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap)).increaseValue(value));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("houseId","householdId","deviceId","year","month","day","index","avg"));
    }
    
}