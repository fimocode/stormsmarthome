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
class Bolt_forecast extends BaseRichBolt {
    public int windows;
    private OutputCollector _collector;
    public long processed = Long.valueOf(0);
    public File output;
    public HashMap < Integer, HashMap <String, HouseData> > data_list = new HashMap< Integer, HashMap <String, HouseData> >();
    public Date lastChange = new Date();
    Stack<HouseData> needSave = new Stack<HouseData>();
    
    public Bolt_forecast(int windows ,File output) {
        this.windows = windows;
        this.output = output;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        if((Long)tuple.getValueByField("end")!=0){
            for(int house_id : data_list.keySet()){
                Stack<String> needClean = new Stack<String>();
                HashMap<String,HouseData> house_data = data_list.get(house_id);
                for(String slice_name : house_data.keySet()){
                    HouseData data = house_data.get(slice_name);
                    if(!data.isSaved()){
                        needSave.push(data);
                    }
                    else if((System.currentTimeMillis()-data.getLastUpdate())>windows*60000){
                        needClean.push(slice_name);
                    }
                }
                for(String slice_name : needClean){
                    house_data.remove(slice_name);
                }
                if(db_store.pushForecastHouseData(needSave)){
                    for(HouseData data : needSave){
                        HashMap <String,HouseData> slice_data = data_list.get(data.getHouse_id());
                        slice_data.put(data.getSliceName(),data.saved());
                        data_list.put(data.getHouse_id(), slice_data);
                    }
                    needSave.clear();
                }
            }
        }
        else{
            Integer house_id        = (Integer) tuple.getValueByField("house_id");
            Double  avg           = (Double) tuple.getValueByField("avg");
            Integer slice_num       = (Integer)tuple.getValueByField("slice_num");
            String year             = (String)tuple.getValueByField("year");
            String month            = (String)tuple.getValueByField("month");
            String day              = (String)tuple.getValueByField("day");
            String date             = year + "/" + month + "/" + day;
            String slice_name = date + " " +  String.format("%02d", Math.floorDiv((slice_num*windows),60)) + ":" +  String.format("%02d", (slice_num*windows)%60) + "->" +  String.format("%02d", Math.floorDiv(((slice_num+1)*windows),60)) + ":" +  String.format("%02d", ((slice_num+1)*windows)%60) ;
            
            HashMap<String,HouseData> house_data = data_list.getOrDefault(house_id, new HashMap<String, HouseData>());
            house_data.put(slice_name, new HouseData(house_id,year,month,day,windows,slice_num,avg));
            data_list.put(house_id, house_data);
            processed++;
        }
    }
}
