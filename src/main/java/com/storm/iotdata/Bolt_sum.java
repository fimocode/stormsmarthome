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
import java.util.HashMap;
import java.util.Map;
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
    private OutputCollector _collector;
    public File output;
    public volatile HashMap < Integer, HashMap <String, HashMap<String, Double> > > data;
    public volatile HashMap < Integer, HashMap <String, Double> > final_data;
    
    public Bolt_sum(HashMap<Integer, HashMap<String, HashMap<String, Double>>> data, HashMap < Integer, HashMap <String, Double> > final_data, File output) {
        this.data = new HashMap<>();
        this.final_data = new HashMap<>();
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
                if(final_data.size()==0){
                    System.out.println("No data recorded");
                }
                else{
                    System.out.println("Writing to " + output.getAbsolutePath());
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
                        HashMap <String, Double> house_data = final_data.get(house);
                        for(Object slice : keySet){
                            bw.write(","+ house_data.get(slice));
                        }
                        bw.write('\n');
                    }
                    Long duration = System.currentTimeMillis() - (Long)tuple.getValueByField("end");
                    if(duration>=3600000){
                        bw.write(String.format("\nTotal,%d hours %d minutes %d seconds", Math.floorDiv(duration,3600000), Math.floorDiv(duration%3600000,60000), ((duration%3600000)%60000)/1000 ));
                    }
                    else if(duration>=60000){
                        bw.write(String.format("\nTotal,%d minutes %d seconds", Math.floorDiv(duration,60000), (duration%60000)/1000 ));
                    }
                    else{
                        bw.write(String.format("\nTotal,%d seconds", duration/1000 ));
                    }
                    bw.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(Bolt_sum.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else{
            Integer house_id     = (Integer) tuple.getValueByField("house_id");
            Double  value        = (Double) tuple.getValueByField("value");
            String household_deviceid = (String)tuple.getValueByField("household_deviceid");
            String slice_name = (String)tuple.getValueByField("slice_name");
            HashMap<String, HashMap<String, Double>> data_house = data.getOrDefault(house_id, new HashMap<String, HashMap<String, Double>>());
            HashMap<String, Double> data_slice = data_house.getOrDefault(slice_name, new HashMap<String, Double>());
            data_slice.put(household_deviceid, value);
            data_house.put(slice_name, data_slice);
            data.put(house_id, data_house);
            Double sum = new Double("0");
            for(String device : data_slice.keySet()){
                sum += data_slice.get(device);
            }
            HashMap<String, Double> result_house = final_data.getOrDefault(house_id, new HashMap<String, Double>());
            result_house.put(slice_name, sum);
            final_data.put(house_id, result_house);
        }
    }
}
