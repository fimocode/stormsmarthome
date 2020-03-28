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
import java.util.Stack;
import java.util.stream.IntStream;

// import javax.swing.JFileChooser;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopo {
    public static void main(String[] args) throws Exception {
        if (!new File("cred.yaml").exists()) {
            System.out.println("Credential file not found!");
        } else {
            db_store.initForecastTable("house_data_forecast_v1");
            db_store.initForecastTable("house_data_forecast_v2");
            int[] window_list = { 120, 60, 30, 20, 15, 10, 5 };
            HashMap<Integer, HashMap<Integer, Forecast>> threads = new HashMap<Integer, HashMap<Integer, Forecast>>();
            for (int windows : window_list) {
                HashMap<Integer, Forecast> windows_thread = threads.getOrDefault(windows, new HashMap<Integer, Forecast>());
                IntStream.range(0, 40).forEachOrdered(n -> {
                    windows_thread.put(n, new Forecast(n, new Date(113, 8, 1), windows));
                    windows_thread.get(n).start();
                });
                threads.put(windows, windows_thread);
                int temp = 0;
                while (true) {
                    temp = 0;
                    boolean done = true;
                    for(int i = 0; i<40; i++) {
                        if (windows_thread.get(i).isAlive()) {
                            done = false;
                            temp += windows_thread.get(i).speed;
                            windows_thread.get(i).speed = 0;
                        }
                        else{
                            windows_thread.put(i, null);
                        }
                    };
                    System.out.printf("\rForecaste: %.2f", (float) temp/2);
                    if (done) {
                        break;
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            };

            // try {
            //     Scanner sc = new Scanner(System.in);
            //     System.out.println("Do you want to purge data");
            //     if(sc.nextLine().charAt(0)=='Y'){
            //         System.out.println("Purging old data");
            //         if(!db_store.purgeData()){
            //             System.out.println("[WARN] DB purge fail");
            //             Thread.sleep(3000);
            //         }
            //         if(!new File("Result").delete()){
            //             System.out.println("[WARN] Clean result folder fail");
            //             Thread.sleep(3000);
            //         }
            //     }
            //     if(!(new File("Result").isDirectory())){
            //         new File("Result").mkdir();
            //     }
            //     // String topoName = "DataAnalize";
            //     // String brokerURL = "tcp://127.0.0.1:1883";
            //     // //Init topic list and window list
            //     // String[] topic_list = new String[40];
            //     // for(int i = 0; i<40; i++){
            //     //     String topic = "house-" + i;
            //     //     topic_list[i] = topic;
            //     // }
            //     // // topic_list = new String[]{"#"};
            //     // int[] window_list = {5,10,15,20,30,60,120};
            //     // TopologyBuilder builder = new TopologyBuilder();
    
            //     // builder.setSpout("trigger", new Spout_trigger(30), 1);
                
            //     // for(String topic : topic_list){
            //     //     //Spout
            //     //     builder.setSpout("spout" + topic, new Spout(brokerURL, topic), 1);
            //     // }
    
            //     // HashMap<String,BoltDeclarer> split_list = new HashMap<String,BoltDeclarer>();
            //     // HashMap<String,BoltDeclarer> avg_list = new HashMap<String,BoltDeclarer>();
            //     // HashMap<String,BoltDeclarer> sum_list = new HashMap<String,BoltDeclarer>();
            //     // for(int window_size : window_list){
            //     //     split_list.put("split" + window_size, builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
            //     //     avg_list.put("avg" + window_size, builder.setBolt("avg" + window_size, new Bolt_avg(window_size, db), 1));
            //     //     sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,new Bolt_sum(new File("Result/output_windows_"+ window_size +"_min.csv")), 1));
            //     // }
                
            //     // for(int window_size : window_list){
            //     //     for(String topic : topic_list){
            //     //         split_list.get("split" + window_size).shuffleGrouping("spout" + topic);
            //     //     }
            //     //     avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
            //     //     sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
            //     //     sum_list.get("sum" + window_size).shuffleGrouping("trigger");
            //     // }

            //     TopologyBuilder builder = new TopologyBuilder();
            //     int[] window_list = {5,10,15,20,30,60,120};
    
            //     builder.setSpout("trigger", new Spout_trigger(30), 1);
            //     builder.setSpout("spout", new Spout("brokerURL", "topic"), 1);
            //     HashMap<String,BoltDeclarer> split_list = new HashMap<String,BoltDeclarer>();
            //     HashMap<String,BoltDeclarer> avg_list = new HashMap<String,BoltDeclarer>();
            //     HashMap<String,BoltDeclarer> sum_list = new HashMap<String,BoltDeclarer>();
            //     for(int window_size : window_list){
            //         split_list.put("split" + window_size, builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
            //         avg_list.put("avg" + window_size, builder.setBolt("avg" + window_size, new Bolt_avg(window_size), 1));
            //         sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,new Bolt_sum(window_size, new File("Result/output_windows_"+ window_size +"_min.csv")), 1));
            //     }
            //     for(int window_size : window_list){
            //         split_list.get("split" + window_size).shuffleGrouping("spout");
            //         avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
            //         sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
            //         avg_list.get("avg" + window_size).shuffleGrouping("trigger");
            //     }
            //     Config conf = new Config();
            //     //conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 50000);
            //     LocalCluster cluster = new LocalCluster(); // create the local cluster
            //     cluster.submitTopology("smarthome", conf, builder.createTopology()); // define the name of mylocal cluster, my configuration object, and my topology
            // } catch (Exception e) {
            //     e.printStackTrace();
            //     new BufferedWriter(new FileWriter(new File("Error.log"),true)).write(new Date().toString() + "|" + e.toString());
            // }
        }
    }
}