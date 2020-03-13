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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author kulz0
 */
class Bolt_sum extends BaseRichBolt {
    public int windows;
    private OutputCollector _collector;
    public long processed = Long.valueOf(0);
    public File output;
    public HashMap<Integer,HashMap<String,HashMap<String, DeviceData> > > data_list = new HashMap<Integer,HashMap<String,HashMap<String, DeviceData> > >();
    public HashMap < Integer, HashMap <String, HouseData> > final_data = new HashMap< Integer, HashMap <String, HouseData> >();
    public Date lastChange = new Date();
    private Long lastProcessed = Long.valueOf(0);
    Stack<HouseData> needSave = new Stack<HouseData>();
    
    public Bolt_sum(int windows ,File output) {
        this.windows = windows;
        this.output = output;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","slice","value"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        BufferedWriter bw     = null;
        if((Long)tuple.getValueByField("end")!=0){
            try {
                int clean = 0;
                int data_size = 0;
                for(Integer house_id : data_list.keySet()){
                    Stack<String> sliceNeedClean = new Stack<String>();
                    HashMap<String, HashMap<String, DeviceData> >house_data = data_list.get(house_id);
                    for(String slice_name : house_data.keySet()){
                        Stack<String> dataNeedClean = new Stack<String>();
                        HashMap<String,DeviceData> slice_data = house_data.get(slice_name);
                        for(String unique_id : slice_data.keySet()){
                            DeviceData data = slice_data.get(unique_id);
                            if((System.currentTimeMillis()-data.getLastUpdate())>(60000*windows)){
                                dataNeedClean.push(unique_id);
                            }
                        }
                        for(String unique_id : dataNeedClean){
                            slice_data.remove(unique_id);
                            clean++;
                        }
                        house_data.put(slice_name, slice_data);
                        if(slice_data.isEmpty()){
                            sliceNeedClean.push(slice_name);
                        }
                    }
                    for(String slice_name : sliceNeedClean){
                        house_data.remove(slice_name);
                        System.out.printf("\n[Bolt_sum_%d] Clean slice %s",windows, slice_name);
                    }
                    data_list.put(house_id, house_data);
                    data_size+=house_data.size();
                }
                System.out.printf("\n[Bolt_sum_%d] Data size %d | Cleaned %d objects",windows,data_size,clean);
                //Calculate sum
                for(Integer house_id : data_list.keySet()){
                    HashMap<String, HashMap<String, DeviceData> >house_data = data_list.get(house_id);
                    for(String slice_name : house_data.keySet()){
                        HashMap<String,DeviceData> slice_data = house_data.get(slice_name);
                        Double sum = Double.valueOf(0);
                        DeviceData temp = slice_data.get(slice_data.keySet().toArray()[0]);
                        String year = temp.getYear();
                        String month = temp.getMonth();
                        String day = temp.getDay();
                        Integer slice_num = temp.getSlice_num();
                        for(String unique_id : slice_data.keySet()){
                            sum += slice_data.get(unique_id).avg;
                        }
                        HashMap<String, HouseData> result_house = final_data.getOrDefault(house_id, new HashMap<String, HouseData>());
                        HouseData h_data = result_house.getOrDefault(slice_name, new HouseData(house_id, year, month, day, slice_num, windows));
                        if(!h_data.isSaved()||!h_data.getValue().equals(sum)){
                            result_house.put(slice_name, h_data.value(sum));
                            final_data.put(house_id, result_house);
                            needSave.push(new HouseData(house_id, year, month, day, slice_num, windows, sum));
                        }
                    }
                }

                if(final_data.size()==0){
                    System.out.println("No data recorded");
                }
                else{
                    System.out.println("\nWriting to " + output.getAbsolutePath());
                    Object[] keySet = final_data.get(final_data.keySet().toArray()[0]).keySet().toArray();
                    Arrays.sort(keySet);
                    bw = new BufferedWriter(new FileWriter(output,false));
                    bw.write("House");
                    for(Object slice: keySet){
                        bw.write(","+slice);
                    }
                    bw.write('\n');
                    for(Integer house : final_data.keySet()){
                        bw.write(String.valueOf(house));
                        HashMap <String, HouseData> house_data = final_data.get(house);
                        for(Object slice : keySet){
                            bw.write(","+ house_data.getOrDefault(slice,new HouseData(house, "", "", "", 0, windows)).getValue());
                        }
                        bw.write('\n');
                    }
                    Long duration = System.currentTimeMillis() - (Long)tuple.getValueByField("end");
                    if(duration>=3600000){
                        bw.write(String.format("\nTotal time,%d hours %d minutes %d seconds,,Total messages,%d", Math.floorDiv(duration,3600000), Math.floorDiv(duration%3600000,60000), ((duration%3600000)%60000)/1000, processed));
                    }
                    else if(duration>=60000){
                        bw.write(String.format("\nTotal time,%d minutes %d seconds,,Total messages,%d", Math.floorDiv(duration,60000), (duration%60000)/1000, processed ));
                    }
                    else{
                        bw.write(String.format("\nTotal time,%d seconds,,Total messages,%d", duration/1000, processed));
                    }
                    if(lastProcessed!=processed){
                        bw.write(String.format("\nTemporal process speed,%.2f (mess/s)", (float) ((processed-lastProcessed)*1000)/(System.currentTimeMillis()-lastChange.getTime())));
                        lastChange = new Date();
                        lastProcessed = processed;
                    }
                    else{
                        bw.write("\nTemporal process speed,0 (mess/s),Not received any messages");
                    }
                    bw.write("\nLast update,"+ new Date().toGMTString());
                    bw.write("\nLast change,"+ lastChange.toGMTString());
                    bw.close();
                }
                System.out.printf("\n[Bolt_sum_%d]Need save to DB %d queries",windows,needSave.size());
                if(needSave.size()!=0){
                    if(db_store.pushHouseData(needSave)){
                        for(HouseData data : needSave){
                            HashMap <String,HouseData> slice_data = final_data.get(data.getHouse_id());
                            slice_data.put(data.getSliceName(),data.saved());
                            final_data.put(data.getHouse_id(), slice_data);
                        }
                        needSave.clear();
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(Bolt_sum.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else{
            Integer house_id        = (Integer) tuple.getValueByField("house_id");
            Double  avg           = (Double) tuple.getValueByField("avg");
            Integer household_id    = (Integer)tuple.getValueByField("household_id");
            Integer device_id       = (Integer)tuple.getValueByField("device_id");
            Integer slice_num       = (Integer)tuple.getValueByField("slice_num");
            String year             = (String)tuple.getValueByField("year");
            String month            = (String)tuple.getValueByField("month");
            String day              = (String)tuple.getValueByField("day");
            String date             = year + "/" + month + "/" + day;
            String household_deviceid = house_id+"_"+device_id;
            String unique_id = String.format("%d_%d_%d_%s_%s_%s_%d", house_id, household_id, device_id, year, month, day, slice_num);
            String slice_name = date + " " +  String.format("%02d", Math.floorDiv((slice_num*windows),60)) + ":" +  String.format("%02d", (slice_num*windows)%60) + "->" +  String.format("%02d", Math.floorDiv(((slice_num+1)*windows),60)) + ":" +  String.format("%02d", ((slice_num+1)*windows)%60) ;
            
            HashMap<String,HashMap<String, DeviceData> > house_data = data_list.getOrDefault(house_id, new HashMap<String,HashMap<String, DeviceData> >());
            HashMap<String, DeviceData> slice_data = house_data.getOrDefault(slice_name, new  HashMap<String, DeviceData>());
            slice_data.put(unique_id, slice_data.getOrDefault(unique_id, new DeviceData(house_id, household_id, device_id, year, month, day, slice_num, windows)).avg(avg));
            house_data.put(slice_name, slice_data);
            data_list.put(house_id, house_data);
            processed++;


            // HashMap<String, HashMap<String, Double>> data_house = data.getOrDefault(house_id, new HashMap<String, HashMap<String, Double>>());
            // HashMap<String, Double> data_slice = data_house.getOrDefault(slice_name, new HashMap<String, Double>());
            // data_slice.put(household_deviceid, value);
            // data_house.put(slice_name, data_slice);
            // data.put(house_id, data_house);
            // Double sum = Double.valueOf(0);
            // for(String device : data_slice.keySet()){
            //     sum += data_slice.get(device);
            // }
            // HashMap<String, Double> result_house = final_data.getOrDefault(house_id, new HashMap<String, Double>());
            // result_house.put(slice_name, sum);
            // final_data.put(house_id, result_house);
            // processed++;
        }
    }
}
