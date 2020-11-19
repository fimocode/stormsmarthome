/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata.models;

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

import com.storm.iotdata.functions.*;

/**
 *
 * @author hiiamlala
 */
public class Bolt_sum extends BaseRichBolt {
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    private OutputCollector _collector;
    public HashMap<String, HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > > allData = new HashMap<String, HashMap<Integer,HashMap<Integer,HashMap<String, DeviceData> > > >();
    public HashMap <Integer, HashMap<String, HouseData> > finalHouseData = new HashMap <Integer, HashMap<String, HouseData> >();
    public HashMap <String, HashMap<String, HouseholdData> > finalHouseholdData = new HashMap <String, HashMap<String, HouseholdData> >();
    
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
        try{
            if(tuple.contains("trigger")){
                if(((++triggerCount)%gap)==0){
                    triggerCount = 0;
                    Long startTime = (Long) tuple.getValueByField("trigger");
                    Long spoutSpeed = (Long) tuple.getValueByField("spoutSpeed");
                    Long spoutLoad = (Long) tuple.getValueByField("spoutLoad");
                    Long spoutTotal = (Long) tuple.getValueByField("spoutTotal");

                    Long startExec = System.currentTimeMillis();
                    Integer allDataSize = 0;
                    Integer finalHouseDataSize = 0;
                    Integer finalHouseholdDataSize = 0;
                    Stack<HouseData> houseDataNeedSave = new Stack<HouseData>();
                    Stack<HouseholdData> householdDataNeedSave = new Stack<HouseholdData>();
                    Stack<HouseData> houseDataNeedClean = new Stack<HouseData>();
                    Stack<HouseholdData> householdDataNeedClean = new Stack<HouseholdData>();
                    Stack<String> timesliceNeedClean = new Stack<String>();

                    // Cal sum
                    for(String timeslice : allData.keySet()){
                        HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > sliceData = allData.get(timeslice);
                        for(Integer houseId : sliceData.keySet()) {
                            Double houseValue = Double.valueOf(0);
                            HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.get(houseId);
                            for(Integer householdId : houseData.keySet()) {
                                Double householdValue = Double.valueOf(0);
                                HashMap<String, DeviceData> householdData = houseData.get(householdId);
                                for(String dataId : householdData.keySet()) {
                                    allDataSize++;
                                    DeviceData data = householdData.get(dataId);
                                    houseValue+=data.getAvg();
                                    householdValue+=data.getAvg();
                                }
                                HouseholdData calHouseholdData = new HouseholdData(houseId, householdId, timeslice, householdValue);
                                HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdData.getOrDefault(calHouseholdData.getHouseholdUniqueId(), new HashMap<String, HouseholdData>());
                                HouseholdData tempHouseholdData = tempFinalHouseholdData.getOrDefault(calHouseholdData.getSliceId(), calHouseholdData);
                                tempHouseholdData.setValue(householdValue);
                                tempFinalHouseholdData.put(calHouseholdData.getSliceId(), tempHouseholdData);
                                finalHouseholdData.put(calHouseholdData.getHouseholdUniqueId(), tempFinalHouseholdData);
                            }
                            HouseData calHouseData = new HouseData(houseId, timeslice, houseValue);
                            HashMap<String, HouseData> tempFinalHouseData = finalHouseData.getOrDefault(calHouseData.getHouseId(), new HashMap<String, HouseData>());
                            HouseData tempHouseData = tempFinalHouseData.getOrDefault(calHouseData.getSliceId(), calHouseData);
                            tempHouseData.setValue(houseValue);
                            tempFinalHouseData.put(calHouseData.getSliceId(), tempHouseData);
                            finalHouseData.put(calHouseData.getHouseId(), tempFinalHouseData);
                        }
                    }

                    //Init data
                    for(Integer houseId : finalHouseData.keySet()) {
                        HashMap<String, HouseData> tempFinalHouseData = finalHouseData.get(houseId);
                        for(String timeslice : tempFinalHouseData.keySet()){
                            finalHouseDataSize++;
                            HouseData houseData = tempFinalHouseData.get(timeslice);
                            if(!houseData.isSaved()){
                                houseDataNeedSave.push(houseData);
                            }
                            else if((System.currentTimeMillis()-houseData.getLastUpdate())>(2*gap*1000)){
                                houseDataNeedClean.push(houseData);
                            }
                        }
                    }

                    for(String uniqueHouseholdId : finalHouseholdData.keySet()) {
                        HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdData.get(uniqueHouseholdId);
                        for(String timeslice : tempFinalHouseholdData.keySet()){
                            finalHouseholdDataSize++;
                            HouseholdData householdData = tempFinalHouseholdData.get(timeslice);
                            if(!householdData.isSaved()){
                                householdDataNeedSave.push(householdData);
                            }
                            else if((System.currentTimeMillis()-householdData.getLastUpdate())>(2*gap*1000)){
                                householdDataNeedClean.push(householdData);
                            }
                        }
                    }

                    for(HouseData houseData : houseDataNeedClean){
                        Timeslice timeslice = houseData.getTimeslice();
                        if(timesliceNeedClean.contains(timeslice.getSliceId())) break;
                        for(HouseholdData householdData : householdDataNeedClean){
                            if(timeslice.isSameTimeslice(householdData.getTimeslice())){
                                timesliceNeedClean.push(timeslice.getSliceId());
                                break;
                            }
                        }
                    }

                    // DB Store
                    if(houseDataNeedSave.size()!=0){
                        if(DB_store.pushHouseData(houseDataNeedSave, new File("./tmp/houseData2db-" + gap + ".lck"))){
                            for(HouseData data : houseDataNeedSave){
                                finalHouseData.get(data.getHouseId()).get(data.getSliceId()).save();
                            }
                        }
                    }
                    if(householdDataNeedSave.size()!=0){
                        if(DB_store.pushHouseHoldData(householdDataNeedSave, new File("./tmp/householdData2db-" + gap + ".lck"))){
                            for(HouseholdData data : householdDataNeedSave){
                                finalHouseholdData.get(data.getHouseholdUniqueId()).get(data.getSliceId()).save();
                            }
                        }
                    }

                    //Wipe unused data
                    for(String timeslice : timesliceNeedClean){
                        allData.remove(timeslice);
                    }

                    for(HouseData houseData : houseDataNeedClean){
                        finalHouseData.get(houseData.getHouseId()).remove(houseData.getSliceId());
                    }

                    for(HouseholdData householdData : householdDataNeedClean) {
                        finalHouseholdData.get(householdData.getHouseholdUniqueId()).remove(householdData.getSliceId());
                    }

                    //Logging
                    Long execTime = System.currentTimeMillis() - startExec;
                    System.out.print(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                    System.out.print(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));

                    try {
                        FileWriter log = new FileWriter(new File("tmp/bolt_sum_"+ gap +".tmp"), false);
                        PrintWriter pwOb = new PrintWriter(log , false);
                        pwOb.flush();
                        log.write(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                        log.write(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                        pwOb.close();
                        log.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            else{
                Integer houseId         = (Integer) tuple.getValueByField("houseId");
                Double  avg             = (Double) tuple.getValueByField("avg");
                Integer householdId     = (Integer)tuple.getValueByField("householdId");
                Integer deviceId        = (Integer)tuple.getValueByField("deviceId");
                Integer index           = (Integer)tuple.getValueByField("index");
                String year             = (String)tuple.getValueByField("year");
                String month            = (String)tuple.getValueByField("month");
                String day              = (String)tuple.getValueByField("day");
                DeviceData tempData = new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap).avg(avg).save();

                HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > > sliceData = allData.getOrDefault(tempData.getSliceId(), new HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > >());
                HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.getOrDefault(houseId, new HashMap<Integer,HashMap<String, DeviceData> >());
                HashMap<String, DeviceData> householdData = houseData.getOrDefault(householdId, new HashMap<String, DeviceData>());
                householdData.put(tempData.getUniqueID(), tempData);
                houseData.put(householdId, householdData);
                sliceData.put(houseId, houseData);
                allData.put(tempData.getSliceId(), sliceData);
            }
            _collector.ack(tuple);
        }
        catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }
}