/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

// import javax.swing.JFileChooser;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopo {
    public static void main(String[] args) throws Exception{
        if(!new File("cred.yaml").exists()){
            System.out.println("Credential file not found!");
        }
        else{
            try {
                Scanner sc = new Scanner(System.in);
                System.out.println("Do you want to purge data");
                if(sc.nextLine().charAt(0)=='Y'){
                    System.out.println("Purging old data");
                    if(!db_store.purgeData()){
                        System.out.println("[WARN] DB purge fail");
                        Thread.sleep(3000);
                    }
                    if(!new File("Result").delete()){
                        System.out.println("[WARN] Clean result folder fail");
                        Thread.sleep(3000);
                    }
                }
                if(!(new File("Result").isDirectory())){
                    new File("Result").mkdir();
                }
                // String topoName = "DataAnalize";
                // String brokerURL = "tcp://127.0.0.1:1883";
                // //Init topic list and window list
                // String[] topic_list = new String[40];
                // for(int i = 0; i<40; i++){
                //     String topic = "house-" + i;
                //     topic_list[i] = topic;
                // }
                // // topic_list = new String[]{"#"};
                // int[] window_list = {5,10,15,20,30,60,120};
                // TopologyBuilder builder = new TopologyBuilder();
    
                // builder.setSpout("trigger", new Spout_trigger(30), 1);
                
                // for(String topic : topic_list){
                //     //Spout
                //     builder.setSpout("spout" + topic, new Spout(brokerURL, topic), 1);
                // }
    
                // HashMap<String,BoltDeclarer> split_list = new HashMap<String,BoltDeclarer>();
                // HashMap<String,BoltDeclarer> avg_list = new HashMap<String,BoltDeclarer>();
                // HashMap<String,BoltDeclarer> sum_list = new HashMap<String,BoltDeclarer>();
                // for(int window_size : window_list){
                //     split_list.put("split" + window_size, builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
                //     avg_list.put("avg" + window_size, builder.setBolt("avg" + window_size, new Bolt_avg(window_size, db), 1));
                //     sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,new Bolt_sum(new File("Result/output_windows_"+ window_size +"_min.csv")), 1));
                // }
                
                // for(int window_size : window_list){
                //     for(String topic : topic_list){
                //         split_list.get("split" + window_size).shuffleGrouping("spout" + topic);
                //     }
                //     avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
                //     sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
                //     sum_list.get("sum" + window_size).shuffleGrouping("trigger");
                // }

                TopologyBuilder builder = new TopologyBuilder();
                int[] window_list = {5,10,15,20,30,60,120};
    
                builder.setSpout("trigger", new Spout_trigger(30), 1);
                builder.setSpout("spout", new Spout("brokerURL", "topic"), 1);
                HashMap<String,BoltDeclarer> split_list = new HashMap<String,BoltDeclarer>();
                HashMap<String,BoltDeclarer> avg_list = new HashMap<String,BoltDeclarer>();
                HashMap<String,BoltDeclarer> sum_list = new HashMap<String,BoltDeclarer>();
                for(int window_size : window_list){
                    split_list.put("split" + window_size, builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
                    avg_list.put("avg" + window_size, builder.setBolt("avg" + window_size, new Bolt_avg(window_size), 1));
                    sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,new Bolt_sum(window_size, new File("Result/output_windows_"+ window_size +"_min.csv")), 1));
                }
                for(int window_size : window_list){
                    split_list.get("split" + window_size).shuffleGrouping("spout");
                    avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
                    sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
                    avg_list.get("avg" + window_size).shuffleGrouping("trigger");
                }
                Config conf = new Config();
                //conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 50000);
                LocalCluster cluster = new LocalCluster(); // create the local cluster
                cluster.submitTopology("smarthome", conf, builder.createTopology()); // define the name of mylocal cluster, my configuration object, and my topology
            } catch (Exception e) {
                e.printStackTrace();
                new BufferedWriter(new FileWriter(new File("Error.log"),true)).write(new Date().toString() + "|" + e.toString());
            }
        }
    }
}