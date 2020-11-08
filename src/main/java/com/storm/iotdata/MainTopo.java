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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopo {
    public static void main(String[] args) throws Exception {
        try {
            Options options = new Options();
            Option opt_purge = new Option("p", "purge", false, "Purge data in DB");
            options.addOption(opt_purge);
            Option opt_init = new Option("i", "init", false, "Init DB");
            options.addOption(opt_init);
            Option opt_broker = new Option("b", "broker", true, "Broker URL");
            options.addOption(opt_broker);
            Option opt_topic_list = new Option("t", "topic", true, "Topic list (split by \",\" )");
            options.addOption(opt_topic_list);
            Option opt_windows_list = new Option("w", "windows", true, "Windows list (split by \",\" )");
            options.addOption(opt_windows_list);

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            CommandLine cmd;

            try {
                cmd = parser.parse(options, args);

                if(cmd.hasOption("purge")){
                    DB_store.purgeData();
                }
                else if(cmd.hasOption("i")){
                    DB_store.initData();
                }
                
                // Init Broker URL
                String brokerURL = "tcp://mqtt-broker:1883";

                if(cmd.hasOption("broker")){
                    brokerURL = "tcp://" + cmd.getOptionValue("broker");
                }

                // Init topic list
                String[] topic_list;
                if(cmd.hasOption("topic")){
                    topic_list = cmd.getOptionValue("topic").split(",");
                }
                else{
                    topic_list = new String[] { "iot-data" };
                }

                // Init windows list
                int[] window_list = new int[20];
                if(cmd.hasOption("windows")){
                    String[] tmp = cmd.getOptionValue("windows").split(",");
                    for(int i = 0; i < tmp.length; i++) {
                        window_list[i] = Integer.parseInt(tmp[i]);
                    }
                }
                else{
                    window_list = new int[] { 1, 5, 10, 15, 20, 30, 60, 120 };
                }

                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("trigger", new Spout_trigger(30), 1);

                for (String topic : topic_list) {
                    // Spout
                    builder.setSpout("spout-" + topic, new Spout(brokerURL, topic), 1);
                }

                // builder.setSpout("spout-", new Spout(brokerURL, "iot-data"), 1);

                HashMap<String, BoltDeclarer> split_list = new HashMap<String, BoltDeclarer>();
                HashMap<String, BoltDeclarer> avg_list = new HashMap<String, BoltDeclarer>();
                HashMap<String, BoltDeclarer> sum_list = new HashMap<String, BoltDeclarer>();
                for (int window_size : window_list) {
                    split_list.put("split" + window_size,
                            builder.setBolt("split" + window_size, new Bolt_split(window_size), 1));
                    avg_list.put("avg" + window_size,
                            builder.setBolt("avg" + window_size, new Bolt_avg(window_size), 1));
                    sum_list.put("sum" + window_size, builder.setBolt("sum" + window_size,
                            new Bolt_sum(window_size, new File("Result/output_windows_" + window_size + "_min.csv")),
                            1));
                }

                for (int window_size : window_list) {
                    for (String topic : topic_list) {
                        split_list.get("split" + window_size).shuffleGrouping("spout-" + topic);
                    }
                    // split_list.get("split" + window_size).shuffleGrouping("spout-");
                    avg_list.get("avg" + window_size).shuffleGrouping("split" + window_size);
                    avg_list.get("avg" + window_size).shuffleGrouping("trigger");
                    sum_list.get("sum" + window_size).shuffleGrouping("avg" + window_size);
                    sum_list.get("sum" + window_size).shuffleGrouping("trigger");
                }

                Config conf = new Config();
                // conf.setDebug(true);
                // conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);
                // conf.setNumWorkers(1);
                // LocalCluster cluster = new LocalCluster(); // create the local cluster
                System.out.println("Sending Topo....");
                StormSubmitter.submitTopology("iot-smarthome", conf, builder.createTopology()); // define the name of
                                                                                                // mylocal cluster, my
                                                                                                // configuration object,
                                                                                                // and my topology
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                formatter.printHelp("utility-name", options);
                System.exit(1);
            }
        } catch (Exception e) {
            BufferedWriter log = new BufferedWriter(new FileWriter(new File("Error.log"), true));
            log.write(new Date().toString() + "|" + e.toString() + "\n");
            log.close();
        }
    }
}