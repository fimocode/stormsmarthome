/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import static org.eclipse.paho.client.mqttv3.MqttClient.generateClientId;

public class Spout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    Long total = Long.valueOf(0);
    String brokerUrl = "localhost";
    String clientId = "";
    MqttClient client;
    String topic = "#";

    public Spout(String broker_url, String topic) {
        this.brokerUrl = broker_url;
        this.topic = topic;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        // this.clientId = generateClientId();
        // try {
        // client = new MqttClient(brokerUrl, clientId);
        // client.setCallback(new MqttCallback() {
        // public void connectionLost(Throwable cause) {
        // System.out.println("Lost connection with MQTT Server.");
        // }

        // public void messageArrived(String topic, MqttMessage message) throws
        // Exception {
        // String[] metric = message.toString().split(",");
        // if (Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
        // _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4],
        // metric[5], metric[6], Long.valueOf(0)));
        // total++;
        // }
        // }

        // public void deliveryComplete(IMqttDeliveryToken token) {
        // }
        // });
        // client.connect();
        // client.subscribe(topic);
        // } catch (Exception e) {
        // System.out.println(e.toString());
        // }
        try {
            int temp = 0;
	    int controller = 1600;
	    int expected = 30000;
            int speed = 0;
            long time = 0;
            long last = System.currentTimeMillis();
            BufferedReader br = new BufferedReader(
                    new FileReader(new File("/root/bigdata/sorted.csv")));
            while (br.ready()) {
                String message = br.readLine();
                String[] metric = message.toString().split(",");
                if (Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
                    _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6],
                            Long.valueOf(0)));
                    total++;
                    speed++;
                    time = System.currentTimeMillis()-last;
                    if(time>2000){
                        System.out.printf("\r\t\t\t\tCurrent speed %.2f\t(%d)", (float)speed*1000/time, controller);
			if((speed*1000/time)<expected){
			    controller+=(expected-speed*1000/time)/10;
			}
			else{
			    controller+=(expected-speed*1000/time)/5;
			}
                        last=System.currentTimeMillis();
                        speed=0;
                    }
                    if (++temp > controller) {
                        try {
                            Thread.sleep(10);
                            temp=0;
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                    System.out.printf("\rReaded: %d", total);
                }
            }  
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Data file not found");
        }
        
    }

    @Override
    /*
     * emits a new tuple into the topology or simply returns if there are no new
     * tuples to emit
     */
    public void nextTuple() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* uses default stream id */
        declarer.declare(new Fields("timestamp", "value", "property", "plug_id","household_id", "house_id", "end"));
    }    
}
