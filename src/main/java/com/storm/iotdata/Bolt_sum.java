/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
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
import org.rocksdb.Slice;

/**
 *
 * @author hiiamlala
 */
class Bolt_sum extends BaseRichBolt {
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    private OutputCollector _collector;
    public long processed = Long.valueOf(0);
    public HashMap<String, HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > > allData = new HashMap<String, HashMap<Integer,HashMap<Integer,HashMap<String, DeviceData> > > >();
    public HashMap <Integer, HashMap<String, HouseData> > finalHouseData = new HashMap <Integer, HashMap<String, HouseData> >();
    public HashMap <Integer, HashMap<String, HouseholdData> > finalHouseholdData = new HashMap <Integer, HashMap<String, HouseholdData> >();
    public Date lastChange = new Date();
    public Integer dataSize = 0;
    private Long lastProcessed = Long.valueOf(0);
    
    public Bolt_sum(int gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("houseId", "year", "month", "day", "index", "value"));
    }

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
                Stack<HouseData> houseDataNeedSave = new Stack<HouseData>();
                Stack<HouseholdData> householdDataNeedSave = new Stack<HouseholdData>();
                Stack<HouseData> houseDataNeedClean = new Stack<HouseData>();
                Stack<HouseholdData> householdDataNeedClean = new Stack<HouseholdData>();
                Stack<DeviceData> deviceDataNeedClean = new Stack<DeviceData>();
                
                
                // for(Integer houseId : allData.keySet()){
                //     Stack<String> sliceNeedClean = new Stack<String>();
                //     HashMap<String, HashMap<String, DeviceData> > houseData = allData.get(houseId);
                //     for(String sliceName : houseData.keySet()){
                //         Stack<String> dataNeedClean = new Stack<String>();
                //         HashMap<String,DeviceData> deviceData = houseData.get(sliceName);
                //         for(String uniqueId : deviceData.keySet()){
                //             DeviceData data = deviceData.get(uniqueId);
                //             if((System.currentTimeMillis()-data.getLastUpdate())>(60000*gap)){
                //                 dataNeedClean.push(uniqueId);
                //             }
                //         }
                //         for(String uniqueId : dataNeedClean){
                //             deviceData.remove(uniqueId);
                //             clean++;
                //         }
                //         houseData.put(sliceName, deviceData);
                //         if(deviceData.isEmpty()){
                //             sliceNeedClean.push(sliceName);
                //         }
                //     }
                //     for(String sliceName : sliceNeedClean){
                //         houseData.remove(sliceName);
                //         System.out.printf("\n[Bolt_sum_%d] Clean slice %s",gap, sliceName);
                //     }
                //     allData.put(houseId, houseData);
                //     dataSize+=houseData.size();
                // }
                // System.out.printf("\n[Bolt_sum_%d] Data size %d | Cleaned %d objects", gap, dataSize, clean);
                // //Calculate sum
                // for(Integer houseId : allData.keySet()){
                //     HashMap<Integer, HashMap<String, DeviceData> > houseData = allData.get(houseId);
                //     for(String sliceName : houseData.keySet()){
                //         HashMap<String,DeviceData> sliceData = houseData.get(sliceName);
                //         Double sum = Double.valueOf(0);
                //         DeviceData temp = sliceData.get(sliceData.keySet().toArray()[0]);
                //         String year = temp.getYear();
                //         String month = temp.getMonth();
                //         String day = temp.getDay();
                //         Integer sliceNum = temp.getSliceNumber();
                //         for(String uniqueId : sliceData.keySet()){
                //             sum += sliceData.get(uniqueId).avg;
                //         }
                //         HashMap<String, HouseData> result_house = finalHouseData.getOrDefault(houseId, new HashMap<String, HouseData>());
                //         HouseData h_data = result_house.getOrDefault(sliceName, new HouseData(houseId, year, month, day, sliceNum, gap));
                //         if(!h_data.isSaved()||!h_data.getValue().equals(sum)){
                //             result_house.put(sliceName, h_data.value(sum));
                //             finalHouseData.put(houseId, result_house);
                //             needSave.push(new HouseData(houseId, year, month, day, sliceNum, gap, sum));
                //         }
                //     }
                // }

                // Init data
                for(String timeslice : allData.keySet()){
                    HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > sliceData = allData.get(timeslice);
                    for(Integer houseId : sliceData.keySet()) {
                        Double houseValue = Double.valueOf(0);
                        HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.get(houseId);
                        for(Integer householdId : houseData.keySet()) {
                            Double householdValue = Double.valueOf(0);
                            HashMap<String, DeviceData> householdData = houseData.get(householdId);
                            for(String dataId : householdData.keySet()) {
                                DeviceData data = householdData.get(dataId);
                                if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate()>(2*gap*1000))) {
                                    deviceDataNeedClean.push(data);
                                }
                                houseValue+=data.getAvg();
                                householdValue+=data.getAvg();
                            }
                            HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdData.getOrDefault(householdId, new HashMap<String, HouseholdData>());
                            HouseholdData tempHouseholdData = tempFinalHouseholdData.getOrDefault(timeslice, new HouseholdData(houseId, householdId, timeslice, householdValue));
                            tempHouseholdData.setValue(householdValue);
                            if(!tempHouseholdData.isSaved()){
                                householdDataNeedSave.push(tempHouseholdData);
                            }
                            else if((System.currentTimeMillis()-tempHouseholdData.getLastUpdate())>(2*gap*1000)){
                                householdDataNeedClean.push(tempHouseholdData);
                            }
                            tempFinalHouseholdData.put(timeslice, tempHouseholdData);
                            finalHouseholdData.put(householdId, tempFinalHouseholdData);
                        }
                        HashMap<String, HouseData> tempFinalHouseData = finalHouseData.getOrDefault(houseId, new HashMap<String, HouseData>());
                        HouseData tempHouseData = tempFinalHouseData.getOrDefault(timeslice, new HouseData(houseId, timeslice, houseValue));
                        tempHouseData.setValue(houseValue);
                        if(!tempHouseData.isSaved()){
                            houseDataNeedSave.push(tempHouseData);
                        }
                        else if((System.currentTimeMillis()-tempHouseData.getLastUpdate())>(2*gap*1000)){
                            houseDataNeedClean.push(tempHouseData);
                        }
                        tempFinalHouseData.put(timeslice, tempHouseData);
                        finalHouseData.put(houseId, tempFinalHouseData);
                    }
                }

                for(DeviceData deviceData : deviceDataNeedClean){
                    allData.get(deviceData.getSliceId());
                }

                System.out.printf("\n[Bolt_sum_%3d] Need save %10d HouseData | %10d HouseholdData.", gap, houseDataNeedSave.size(), householdDataNeedSave.size());
                System.out.printf("\n[Bolt_sum_%3d] Cleaned %10d HouseData | %10d HouseholdData | %10d DeviceData", gap, houseDataNeedClean.size(), householdDataNeedClean.size(), deviceDataNeedClean.size());

                try {
                    FileWriter log = new FileWriter(new File("tmp/bolt_sum_"+ gap +".tmp"), false);
                    PrintWriter pwOb = new PrintWriter(log , false);
                    pwOb.flush();
                    log.write(String.format("[Bolt_sum_%3d] Need save %10d HouseData | %10d HouseholdData | Cleaned %10d HouseData | %10d HouseholdData | %10d DeviceData",gap, houseDataNeedSave.size(), householdDataNeedSave.size(),  houseDataNeedClean.size(), householdDataNeedClean.size(), deviceDataNeedClean.size()));
                    pwOb.close();
                    log.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                // DB Store
                if(houseDataNeedSave.size()!=0){
                    if(DB_store.pushHouseData(houseDataNeedSave, new File("./tmp/house2db-" + gap + ".lck"))){
                        for(HouseData data : houseDataNeedSave){
                            HashMap<String, HouseData> tempFinalHouseData = finalHouseData.get(data.getHouseId());
                            HouseData tempHouseData = tempFinalHouseData.get(data.getSliceId());
                            tempHouseData.save();
                            tempFinalHouseData.put(tempHouseData.getSliceId(), tempHouseData);
                            finalHouseData.put(tempHouseData.getHouseId(), tempFinalHouseData);
                        }
                    }
                }
                if(householdDataNeedSave.size()!=0){
                    if(DB_store.pushHouseHoldData(householdDataNeedSave, new File("./tmp/household2db-" + gap + ".lck"))){
                        for(HouseholdData data : householdDataNeedSave){
                            HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdData.get(data.getHouseholdId());
                            HouseholdData tempHouseholdData = tempFinalHouseholdData.get(data.getSliceId());
                            tempHouseholdData.save();
                            tempFinalHouseholdData.put(tempHouseholdData.getSliceId(), tempHouseholdData);
                            finalHouseholdData.put(tempHouseholdData.getHouseId(), tempFinalHouseholdData);
                        }
                    }
                }

                System.out.println(String.format("\n[Bolt_sum_"+ gap +"] Temporal process speed %.2f (mess/s)", (float) ((processed-lastProcessed)*1000)/(System.currentTimeMillis()-lastChange.getTime())));
            }
        }
        else{
            Integer houseId         = (Integer) tuple.getValueByField("houseId");
            Double  avg             = (Double) tuple.getValueByField("avg");
            Integer householdId     = (Integer)tuple.getValueByField("householdId");
            Integer deviceId        = (Integer)tuple.getValueByField("deviceId");
            Integer index     = (Integer)tuple.getValueByField("index");
            String year             = (String)tuple.getValueByField("year");
            String month            = (String)tuple.getValueByField("month");
            String day              = (String)tuple.getValueByField("day");
            DeviceData tempData = new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap).avg(avg).saved();
            Timeslice timeslice = new Timeslice(year, month, day, index, gap);

            HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > > sliceData = allData.getOrDefault(timeslice.getSliceId(), new HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > >());
            HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.getOrDefault(houseId, new HashMap<Integer,HashMap<String, DeviceData> >());
            HashMap<String, DeviceData> householdData = houseData.getOrDefault(householdId, new HashMap<String, DeviceData>());
            householdData.put(tempData.getUniqueID(), tempData);
            houseData.put(householdId, householdData);
            sliceData.put(houseId, houseData);
            allData.put(timeslice.getSliceId(), sliceData);
            processed++;
        }
        _collector.ack(tuple);
    }
}