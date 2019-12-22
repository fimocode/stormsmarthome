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
// import javax.swing.JFileChooser;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopo {
    public static void main(String[] args) throws Exception{
        try {
            if(!(new File("Result").isDirectory())){
                new File("Result").mkdir();
            }
            HashMap < Integer, HashMap <String, HashMap<String, Double> > > data = new HashMap<Integer, HashMap<String, HashMap<String, Double>>>();
            HashMap <Integer, HashMap<String, HashMap<Long, HashMap<String, Double > > > > map_house = new HashMap<Integer, HashMap<String, HashMap<Long, HashMap<String, Double>>>>();
            HashMap < Integer, HashMap <String, Double> > final_data = new HashMap<Integer, HashMap<String, Double>>();
            String topoName = "DataAnalize";
            String brokerURL = "tcp://127.0.0.1:1883";
            //Init topic list and window list
            String[] topic_list = new String[40];
            for(int i = 0; i<40; i++){
                String topic = "house-" + i;
                topic_list[i] = topic;
            }
            // topic_list = new String[]{"#"};
            int[] window_list = {5,10,15,20,30,60,120};
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("trigger", new Spout_trigger(30), 1);
            
            for(String topic : topic_list){
                //Spout
                builder.setSpout("spout" + topic, new Spout(brokerURL, topic), 1);
            }

            HashMap<String,BoltDeclarer> split_list = new HashMap<String,BoltDeclarer>();
            HashMap<String,BoltDeclarer> avg_list = new HashMap<String,BoltDeclarer>();
            HashMap<String,BoltDeclarer> sum_list = new HashMap<String,BoltDeclarer>();
            for(int window_size : window_list){
                split_list.put("split" + window_size, builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
                avg_list.put("avg" + window_size, builder.setBolt("avg" + window_size, new Bolt_avg(window_size, map_house), 1));
                sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,new Bolt_sum(data, final_data, new File("Result/output_windows_"+ window_size +"_min.csv")), 1));
                for(String topic : topic_list){
                    split_list.get("split" + window_size).shuffleGrouping("spout" + topic);
                    avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
                    sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
                }
                sum_list.get("sum" + window_size).shuffleGrouping("trigger");
            }

            Config conf = new Config();
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);
            LocalCluster cluster = new LocalCluster(); // create the local cluster
            cluster.submitTopology("smarthome", conf, builder.createTopology()); // define the name of mylocal cluster, my configuration object, and my topology


            // File outputDir;
            // JFileChooser chooser = new JFileChooser();
            // chooser.setDialogTitle("Select output folder");
            // chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            // chooser.setAcceptAllFileFilterUsed(false);
            // int returnVal = chooser.showOpenDialog(null);
            // if(returnVal == JFileChooser.APPROVE_OPTION) {
            //     System.out.println("You chose to open this file: " + chooser.getSelectedFile().getParent());
            //     outputDir = chooser.getSelectedFile();

                // builder.setSpout("spout", new Spout(brokerURL, "house-0"), 1);

                // builder.setBolt("split5", new Bolt_split(5), 1).shuffleGrouping("spout");
                // builder.setBolt("avg5", new Bolt_avg(5, map_house), 1).shuffleGrouping("split5");
                // builder.setBolt("split10", new Bolt_split(10), 1).shuffleGrouping("spout");
                // builder.setBolt("avg10", new Bolt_avg(10, map_house), 1).shuffleGrouping("split10");
                // builder.setBolt("split15", new Bolt_split(15), 1).shuffleGrouping("spout");
                // builder.setBolt("avg15", new Bolt_avg(15, map_house), 1).shuffleGrouping("split15");
                // builder.setBolt("split20", new Bolt_split(20), 1).shuffleGrouping("spout");
                // builder.setBolt("avg20", new Bolt_avg(20, map_house), 1).shuffleGrouping("split20");
                // builder.setBolt("split30", new Bolt_split(30), 1).shuffleGrouping("spout");
                // builder.setBolt("avg30", new Bolt_avg(30, map_house), 1).shuffleGrouping("split30");
                // builder.setBolt("split60", new Bolt_split(60), 1).shuffleGrouping("spout");
                // builder.setBolt("avg60", new Bolt_avg(60, map_house), 1).shuffleGrouping("split60");
                // builder.setBolt("split120", new Bolt_split(120), 1).shuffleGrouping("spout");
                // builder.setBolt("avg120", new Bolt_avg(120, map_house), 1).shuffleGrouping("split120");

                // builder.setBolt("sum5",new Bolt_sum(data, final_data, new File("Result/output_windows_5_min.csv")), 1).shuffleGrouping("avg5");
                // builder.setBolt("sum10",new Bolt_sum(data, final_data, new File("Result/output_windows_10_min.csv")), 1).shuffleGrouping("avg10");
                // builder.setBolt("sum15",new Bolt_sum(data, final_data, new File("Result/output_windows_15_min.csv")), 1).shuffleGrouping("avg15");
                // builder.setBolt("sum20",new Bolt_sum(data, final_data, new File("Result/output_windows_20_min.csv")), 1).shuffleGrouping("avg20");
                // builder.setBolt("sum30",new Bolt_sum(data, final_data, new File("Result/output_windows_30_min.csv")), 1).shuffleGrouping("avg30");
                // builder.setBolt("sum60",new Bolt_sum(data, final_data, new File("Result/output_windows_60_min.csv")), 1).shuffleGrouping("avg60");
                // builder.setBolt("sum120",new Bolt_sum(data, final_data, new File("Result/output_windows_120_min.csv")), 1).shuffleGrouping("avg120");
                // if (args != null && args.length > 1) {
                //     // conf.setNumWorkers(Integer.parseInt("10"));
                //     StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
                // } else {
                    //conf.setDebug(false); // enable Debug mode
                // }
            // }
            // else{
            //     System.out.print("Wrong format");
            // }
        } catch (Exception e) {
            new BufferedWriter(new FileWriter(new File("Error.log"),true)).write(new Date().toString() + "|" + e.toString());
        }
    }
}