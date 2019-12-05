/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private File input_file;
    long start;
    Boolean done = false;

    public Spout (File input_file) {
        this.start = System.currentTimeMillis();
        this.input_file = input_file;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    /* emits a new tuple into the topology or simply returns if there are no new tuples to emit */
    public void nextTuple( ) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input_file)));
            String line;
            int inc = 0;
//            while ((line=br.readLine()) != null && inc < (counter) ){ inc ++; }
//            while ((line=br.readLine()) != null && inc < (counter+10) ) {
//                String[] metric = line.split(",");
//                if(Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
//                       _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6]));
//                }
//                inc ++;
//            } counter += 10; // emit 10 lines
            while((!done) && ((line = br.readLine()) != null)){
                String[] metric = line.split(",");
                if(Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
                    _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6], new Long("0")));
                }
//                Thread.sleep(1);
                System.out.print("\rSended: " + ++inc);
            }
            if(!done){
                _collector.emit(new Values("0", "0", "0", "0", "0", "0", start));
                done=true;
            }
            br.close(); 
        }		
        catch (Exception e){
                System.out.println(e.toString());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* uses default stream id */
        declarer.declare(new Fields("timestamp", "value", "property", "plug_id","household_id", "house_id", "end"));
    }    
}
