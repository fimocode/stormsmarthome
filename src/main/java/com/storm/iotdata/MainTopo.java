/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopo {
    public static void main(String[] args) throws Exception{
        HashMap < Integer, HashMap <String, HashMap<String, Double> > > data = new HashMap<Integer, HashMap<String, HashMap<String, Double>>>();
        HashMap <Integer, HashMap<String, HashMap<Long, HashMap<String, Double > > > > map_house = new HashMap<Integer, HashMap<String, HashMap<Long, HashMap<String, Double>>>>();
        HashMap < Integer, HashMap <String, Double> > final_data = new HashMap<Integer, HashMap<String, Double>>();
        String topoName = "DataAnalize";
        File inputFile;
        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter filter = new FileNameExtensionFilter("CSV","csv");
        chooser.setFileFilter(filter);
        int returnVal = chooser.showOpenDialog(null);
        if(returnVal == JFileChooser.APPROVE_OPTION) {
            System.out.println("You chose to open this file: " +
                    chooser.getSelectedFile().getParent());
            inputFile = chooser.getSelectedFile();
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("spout", new Spout(inputFile), 1);
            builder.setBolt("split5", new Bolt_split(5), 1).shuffleGrouping("spout");
            builder.setBolt("avg5", new Bolt_avg(5, map_house), 1).shuffleGrouping("split5");
            builder.setBolt("split10", new Bolt_split(10), 1).shuffleGrouping("spout");
            builder.setBolt("avg10", new Bolt_avg(10, map_house), 1).shuffleGrouping("split10");
            builder.setBolt("split15", new Bolt_split(15), 1).shuffleGrouping("spout");
            builder.setBolt("avg15", new Bolt_avg(15, map_house), 1).shuffleGrouping("split15");
            builder.setBolt("split20", new Bolt_split(20), 1).shuffleGrouping("spout");
            builder.setBolt("avg20", new Bolt_avg(20, map_house), 1).shuffleGrouping("split20");
            builder.setBolt("split30", new Bolt_split(30), 1).shuffleGrouping("spout");
            builder.setBolt("avg30", new Bolt_avg(30, map_house), 1).shuffleGrouping("split30");
            builder.setBolt("split60", new Bolt_split(60), 1).shuffleGrouping("spout");
            builder.setBolt("avg60", new Bolt_avg(60, map_house), 1).shuffleGrouping("split60");
            builder.setBolt("split120", new Bolt_split(120), 1).shuffleGrouping("spout");
            builder.setBolt("avg120", new Bolt_avg(120, map_house), 1).shuffleGrouping("split120");
            if(!(new File( inputFile.getParent() + "\\Result_" + inputFile.getName()).isDirectory())){
                new File( inputFile.getParent() + "\\Result_" + inputFile.getName()).mkdir();
            }
            builder.setBolt("sum5",new Bolt_sum(data, final_data, new File( inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_5_min.csv")), 1).shuffleGrouping("avg5");
            builder.setBolt("sum10",new Bolt_sum(data, final_data, new File(inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_10_min.csv")), 1).shuffleGrouping("avg10");
            builder.setBolt("sum15",new Bolt_sum(data, final_data, new File(inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_15_min.csv")), 1).shuffleGrouping("avg15");
            builder.setBolt("sum20",new Bolt_sum(data, final_data, new File(inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_20_min.csv")), 1).shuffleGrouping("avg20");
            builder.setBolt("sum30",new Bolt_sum(data, final_data, new File(inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_30_min.csv")), 1).shuffleGrouping("avg30");
            builder.setBolt("sum60",new Bolt_sum(data, final_data, new File(inputFile.getParent() + "\\Result_" + inputFile.getName() + "\\output_windows_60_min.csv")), 1).shuffleGrouping("avg60");
            builder.setBolt("sum120",new Bolt_sum(data, final_data, new File(inputFile.getParent()+ "\\Result_" + inputFile.getName() + "\\output_windows_120_min.csv")), 1).shuffleGrouping("avg120");
            Config conf = new Config(); // define a configuration object
            if (args != null && args.length > 1) {
                conf.setNumWorkers(Integer.parseInt("10"));
                StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
            } else {
                // test the topology in local mode

                //conf.setDebug(false); // enable Debug mode
                conf.put("input_file", inputFile.getName());
                conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

                LocalCluster cluster = new LocalCluster(); // create the local cluster
                cluster.submitTopology("debs-topologie", conf, builder.createTopology()); // define the name of mylocal cluster, my configuration object, and my topology
//                Thread.sleep(20000); // sleep for two seconds
//                cluster.shutdown(); // and then shuts down the cluster
            }
        }
        else{
            System.out.print("Wrong format");
        }
    }
}