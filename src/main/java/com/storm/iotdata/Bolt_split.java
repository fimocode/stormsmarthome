
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
 * @author hiiamlala
 */
public class Bolt_split extends BaseRichBolt {
    private StormConfig config;
    private int windows = 5;
    public Bolt_split(int windows, StormConfig config) {
        this.windows = windows;
        this.config = config;
    }

    // output collector
    private OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id", "household_id", "device_id", "year", "month", "day", "slice_num", "value"));
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
        // Integer property     = Integer.parseInt((String)tuple.getValueByField("property"));
        // Timestamp stamp = new Timestamp(timestamp);
        Date date = new Date(timestamp*1000);
        String year = Integer.toString(1900 + date.getYear());;
        String month = String.format("%02d", (1+date.getMonth()));
        String day = String.format("%02d", date.getDate()) ;
        Long time = (date.getTime()%86400000);
        int slice_num = (int) Math.floorDiv(time,(windows*60000));
        _collector.emit(new Values(house_id, household_id, plug_id, year, month, day, slice_num, value));
        _collector.ack(tuple);
    }
}