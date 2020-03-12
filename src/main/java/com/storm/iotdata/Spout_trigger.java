package com.storm.iotdata;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout_trigger extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private long start = System.currentTimeMillis();
    public int interval = 1;

    public Spout_trigger(int interval){
        this.interval = interval;
        this.start = System.currentTimeMillis();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(interval*1000);
            _collector.emit(new Values(0,0,0,"","","",(long)0,(double)0,start)); // Trigger signal to write data to file after 1 min
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("house_id", "household_id", "device_id", "year", "month", "date", "slice_num", "value", "end"));
    }
}