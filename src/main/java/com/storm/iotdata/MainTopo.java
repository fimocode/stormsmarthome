/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
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
        StormConfig config = new StormConfig();
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

                if(cmd.hasOption("purge") || config.isCleanDatabase()){
                    DB_store.purgeData();
                }
                else{
                    DB_store.initData();
                }
                
                // Init Broker URL
                if(cmd.hasOption("broker")){
                    config.setSpoutBrokerURL("tcp://" + cmd.getOptionValue("broker"));
                }

                // Init topic list
                if(cmd.hasOption("topic")){
                    config.setSpoutTopicList(Arrays.asList(cmd.getOptionValue("topic").split(",")));
                }

                // Init windows list
                
                if(cmd.hasOption("windows")){
                    Integer[] windowList = new Integer[100];
                    String[] tmp = cmd.getOptionValue("windows").split(",");
                    for(int i = 0; i < tmp.length; i++) {
                        windowList[i] = Integer.parseInt(tmp[i]);
                    }
                    config.setWindowList(Arrays.asList(windowList));
                }

                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("trigger", new Spout_trigger(30), 1);

                for (String topic : config.getSpoutTopicList()) {
                    // Spout
                    builder.setSpout("spout-" + topic, new Spout(config.getSpoutBrokerURL(), topic), 1);
                }

                // builder.setSpout("spout-", new Spout(brokerURL, "iot-data"), 1);

                HashMap<String, BoltDeclarer> splitList = new HashMap<String, BoltDeclarer>();
                HashMap<String, BoltDeclarer> avgList = new HashMap<String, BoltDeclarer>();
                HashMap<String, BoltDeclarer> sumList = new HashMap<String, BoltDeclarer>();
                for (Integer windowSize : config.getWindowList()) {
                    splitList.put("split" + windowSize,
                            builder.setBolt("split" + windowSize, new Bolt_split(windowSize, config), 1));
                    avgList.put("avg" + windowSize,
                            builder.setBolt("avg" + windowSize, new Bolt_avg(windowSize, config), 1));
                    sumList.put("sum" + windowSize, builder.setBolt("sum" + windowSize,
                            new Bolt_sum(windowSize, config),
                            1));
                }

                for (Integer windowSize : config.getWindowList()) {
                    for (String topic : config.getSpoutTopicList()){
                        splitList.get("split" + windowSize).shuffleGrouping("spout-" + topic);
                    }
                    // split_list.get("split" + window_size).shuffleGrouping("spout-");
                    avgList.get("avg" + windowSize).shuffleGrouping("split" + windowSize);
                    avgList.get("avg" + windowSize).shuffleGrouping("trigger");
                    sumList.get("sum" + windowSize).shuffleGrouping("avg" + windowSize);
                    sumList.get("sum" + windowSize).shuffleGrouping("trigger");
                }

                Config conf = new Config();
                // conf.setDebug(true);
                // conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);
                // conf.setNumWorkers(1);
                // LocalCluster cluster = new LocalCluster(); // create the local cluster
                System.out.println("Sending Topo....");
                StormSubmitter.submitTopology(config.getTopologyName(), conf, builder.createTopology()); // define the name of
                                                                                                // mylocal cluster, my
                                                                                                // configuration object,
                                                                                                // and my topology
                System.out.println("Sent");
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                formatter.printHelp("utility-name", options);
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            BufferedWriter log = new BufferedWriter(new FileWriter(new File("Error.log"), true));
            log.write(new Date().toString() + "|" + e.toString() + "\n");
            log.close();
        }
    }
}