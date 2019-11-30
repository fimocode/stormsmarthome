/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.util.Date;
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
public class Bolt_split extends BaseRichBolt {
    private int windows = 5;
    public Bolt_split(int windows) {
        this.windows = windows;
    }

    // output collector
    private OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id","household_deviceid", "date","slice_num","index","value","end"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer house_id     = Integer.parseInt((String)tuple.getValueByField("house_id"));
        Integer household_id = Integer.parseInt((String)tuple.getValueByField("household_id"));
        Integer plug_id      = Integer.parseInt((String)tuple.getValueByField("plug_id"));
        Long    timestamp    = Long.parseLong((String)tuple.getValueByField("timestamp"));
        Double  value        = Double.parseDouble((String)tuple.getValueByField("value"));
        Integer property     = Integer.parseInt((String)tuple.getValueByField("property"));
//        Timestamp stamp = new Timestamp(timestamp);
        Date date = new Date(timestamp*1000);
        String day = (1900+date.getYear()) + "/" + String.format("%02d", (1+date.getMonth())) + "/" +  String.format("%02d", date.getDate()) ;
        Long time = (date.getTime()%86400000);
        long slice_num = (int) Math.floorDiv(time,(windows*60000));
        Long index = time%(windows*60000);
        String household_deviceid = household_id + "_" + plug_id;
        if((Boolean)tuple.getValueByField("end")){
            _collector.emit(new Values(house_id, household_deviceid, day, slice_num, index, value, true));
        }
        else{
            _collector.emit(new Values(house_id, household_deviceid, day, slice_num, index, value, false));
        }
    }
}