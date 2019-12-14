/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

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
    long start;
    Long total = new Long("0");
    String brokerUrl = "localhost";
    String clientId = "";
    MqttClient client;
    long lastW = new Long("0");
    String topic = "#";

    public Spout (String broker_url, String topic) {
        this.start = System.currentTimeMillis();
        this.lastW = start;
        this.brokerUrl = broker_url;
        this.clientId = generateClientId();
        this.topic = topic;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            client = new MqttClient(brokerUrl, clientId);
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    System.out.println("Lost connection with MQTT Server.");
                }

                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String[] metric = message.toString().split(",");
                    if(Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
                        _collector.emit(new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6], new Long("0")));
                        total++;
                    }
                    // System.out.print("\rReceived: "+ total);
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            client.connect();
            client.subscribe(topic);
        }		
        catch (Exception e){
                System.out.println(e.toString());
        }
    }

    @Override
    /* emits a new tuple into the topology or simply returns if there are no new tuples to emit */
    public void nextTuple( ) {
        if((System.currentTimeMillis()-lastW)>300000){ //Trigger signal to write data to file after 5 min
            _collector.emit(new Values("0", "0", "0", "0", "0", "0", start));
            lastW = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* uses default stream id */
        declarer.declare(new Fields("timestamp", "value", "property", "plug_id","household_id", "house_id", "end"));
    }    
}
