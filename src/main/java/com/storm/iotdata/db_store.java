package com.storm.iotdata;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.generated.DistributedRPCInvocations.AsyncProcessor.result;
import org.yaml.snakeyaml.Yaml;

public class db_store {
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

    public static void main(String[] args) {
        DeviceData data = new DeviceData(1, 2, 3, "4", "5", "6", 7, 8);
        data.increaseValue(new Double(200));
        // db_store.purgeData();
        Stack<DeviceData> data_list = new Stack<DeviceData>();
        data_list.push(data);
        db_store.pushDeviceData(data_list);
    }

    public static boolean purgeData(){
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            int rs = stmt.executeUpdate("drop database iot_data");
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if(db_store.initData())
                return true;
            else
                return false;
        }
    }

    public static boolean initData(){
        try{
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create database iot_data");
            stmt.execute("use iot_data");
            stmt.executeUpdate("create table device_data (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, windows INT NOT NULL, slice_num INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, year, month, day, windows, slice_num))");
            stmt.executeUpdate("create table house_data(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,windows INT NOT NULL, slice_num INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, windows, slice_num))");
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static Stack<HouseData> pushHouseData(Stack<HouseData> data_list){
        Stack<HouseData> result = new Stack<HouseData>();
        try{
            //Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            //Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for(HouseData data : data_list){
                PreparedStatement temp_sql = conn.prepareStatement("insert into house_data (house_id,year,month,day,windows,slice_num,value) values (?,?,?,?,?,?,?) on duplicate key update value=?", Statement.RETURN_GENERATED_KEYS);
                temp_sql.setInt(1, data.getHouse_id());
                temp_sql.setString(2, data.getYear());
                temp_sql.setString(3, data.getMonth());
                temp_sql.setString(4, data.getDay());
                temp_sql.setInt(5, data.getWindows());
                temp_sql.setInt(6, data.getSlice_num());
                temp_sql.setDouble(7, data.getValue());
                temp_sql.setDouble(8, data.getValue());
                temp_sql.executeUpdate();
                result.push(data);
            }
            conn.close();
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            return result;
        }
    }

    public static Stack<String> pushDeviceData(Stack<DeviceData> data_list){
        Stack<String> result = new Stack<String>();
        try{
            System.out.printf("\n\n\nStoring %d\n\n\n",data_list.size());
            //Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            //Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for(DeviceData data : data_list){
                PreparedStatement temp_sql = conn.prepareStatement("insert into device_data (house_id,household_id,device_id,year,month,day,windows,slice_num,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=?, count=?, avg=?", Statement.RETURN_GENERATED_KEYS);
                temp_sql.setInt(1, data.getHouse_id());
                temp_sql.setInt(2, data.getHousehold_id());
                temp_sql.setInt(3, data.getDevice_id());
                temp_sql.setString(4, data.getYear());
                temp_sql.setString(5, data.getMonth());
                temp_sql.setString(6, data.getDay());
                temp_sql.setInt(7, data.getWindows());
                temp_sql.setInt(8, data.getSlice_num());
                temp_sql.setDouble(9, data.getValue());
                temp_sql.setDouble(10, data.getCount());
                temp_sql.setDouble(11, data.getAvg());
                temp_sql.setDouble(12, data.getValue());
                temp_sql.setDouble(13, data.getCount());
                temp_sql.setDouble(14, data.getAvg());
                temp_sql.executeUpdate();
                result.push(data.getUniqueID());
            }
            conn.close();
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            return result;
        }
    }

    public static boolean saveData(DeviceData data){
        try{
            //Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://"+obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            //Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            PreparedStatement temp_sql = conn.prepareStatement("insert into device_data (house_id,household_id,device_id,year,month,day,windows,slice_num,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=?, count=?, avg=?", Statement.RETURN_GENERATED_KEYS);
            temp_sql.setInt(1, data.getHouse_id());
            temp_sql.setInt(2, data.getHousehold_id());
            temp_sql.setInt(3, data.getDevice_id());
            temp_sql.setString(4, data.getYear());
            temp_sql.setString(5, data.getMonth());
            temp_sql.setString(6, data.getDay());
            temp_sql.setInt(7, data.getWindows());
            temp_sql.setInt(8, data.getSlice_num());
            temp_sql.setDouble(9, data.getValue());
            temp_sql.setDouble(10, data.getCount());
            temp_sql.setDouble(11, data.getAvg());
            temp_sql.setDouble(12, data.getValue());
            temp_sql.setDouble(13, data.getCount());
            temp_sql.setDouble(14, data.getAvg());
            temp_sql.executeUpdate();
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    };

    public static Connection getConnection(String dbURL, String userName, String password) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (Exception ex) {
            System.out.println("connect failure!");
            ex.printStackTrace();
        }
        return conn;
    }

}
