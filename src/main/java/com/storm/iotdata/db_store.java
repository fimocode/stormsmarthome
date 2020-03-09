package com.storm.iotdata;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
public class db_store implements Serializable{
    // private Connection conn;
    // public db_store(String dbURL, String userName, String password){
    //     try {
    //         Class.forName("com.mysql.jdbc.Driver");
    //         conn = DriverManager.getConnection(dbURL, userName, password);
    //         System.out.println("connect successfully!");
    //     } catch (Exception ex) {
    //         System.out.println("connect failure!");
    //         ex.printStackTrace();
    //     }
    // }

    public boolean purgeData(){
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            System.out.println("connect successfully!");
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate("drop database iot_data");
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if(this.initData())
                return true;
            else
                return false;
        }
    }

    public boolean initData(){
        try{
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            System.out.println("connect successfully!");
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create database iot_data");
            stmt.execute("use iot_data");
            stmt.executeUpdate("create table device_data (house_id INT UNSIGNED NOT NULL, household_deviceid VARCHAR(30) NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_size INT NOT NULL, slice_num INT NOT NULL, total DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
            stmt.executeUpdate("create table house_data(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_size INT NOT NULL, slice_num INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean pushData(int windows, HashMap <Integer, HashMap<String, HashMap<String, HashMap<String, HashMap<String, HashMap<Long, HashMap<String, Double > > > > > > > map_house){
        try{
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            System.out.println("connect successfully!");
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for(int house_id : map_house.keySet()){
                for(String device_id : map_house.get(house_id).keySet()){
                    for(String year : map_house.get(house_id).get(device_id).keySet()){
                        for(String month : map_house.get(house_id).get(device_id).get(year).keySet()){
                            for(String day : map_house.get(house_id).get(device_id).get(year).get(month).keySet()){
                                for(long slice_num : map_house.get(house_id).get(device_id).get(year).get(month).get(day).keySet()){
                                    HashMap<String, Double> data = map_house.get(house_id).get(device_id).get(year).get(month).get(day).get(slice_num);
                                    String sql = String.format("insert into iot_data(house_id,household_deviceid,year,month,day,slice_size,slice_num,total,count,avg) values (%d, %s, %s, %s, %s, %d, %d, %d, %d, %d)", house_id, device_id, year, month, day, windows, slice_num, data.get("total"), data.get("count"), data.get("avg"));
                                    stmt.executeUpdate(sql);
                                }
                            }
                        }
                    }
                }
            }
            System.out.printf("\nSaved to DB (%.2f s)\n",(System.currentTimeMillis()-start)/60000);
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static Connection getConnection(String dbURL, String userName, String password) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, userName, password);
            System.out.println("connect successfully!");
        } catch (Exception ex) {
            System.out.println("connect failure!");
            ex.printStackTrace();
        }
        return conn;
    }
}
