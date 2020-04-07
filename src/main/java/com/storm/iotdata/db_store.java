package com.storm.iotdata;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.generated.DistributedRPCInvocations.AsyncProcessor.result;
import org.yaml.snakeyaml.Yaml;

public class db_store {

    private Connection conn;

    public db_store() {
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (Exception ex) {
            System.out.println("connect failure!");
            ex.printStackTrace();
        }
    }

    public static boolean purgeData() {
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
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
            if (db_store.initData())
                return true;
            else
                return false;
        }
    }

    public static boolean initData() {
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create database iot_data");
            stmt.execute("use iot_data");
            stmt.executeUpdate(
                    "create table device_data (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, windows INT NOT NULL, slice_num INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, year, month, day, windows, slice_num))");
            stmt.executeUpdate(
                    "create table house_data(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,windows INT NOT NULL, slice_num INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, windows, slice_num))");
            stmt.executeUpdate(
                    "create table house_data_forecast(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,windows INT NOT NULL, slice_num INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, windows, slice_num))");
            stmt.excuteUpdate("create table forecast_meta_data(version VARCHAR(4) NOT NULL, windows INT NOT NULL, count INT UNSIGNED DEFAULT 0, mean DOUBLE UNSIGNED DEFAULT 0, variance DOUBLE UNSIGNED DEFAULT 0, standart_deviation DOUBLE UNSIGNED DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(windows, version))");
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean initForecastTable(String table) {
        try {
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            stmt.execute("drop table if exists " + table);
            stmt.executeUpdate(
                    "create table "+ table +"(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,windows INT NOT NULL, slice_num INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, windows, slice_num))");
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseData(Stack<HouseData> data_list) {
        try {
            // Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            // Init SQL
            Long start = System.currentTimeMillis();
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            String sql = "insert into house_data (house_id,year,month,day,windows,slice_num,avg) values ";
            for (HouseData data : data_list) {
                PreparedStatement temp_sql = conn.prepareStatement("(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                temp_sql.setInt(1, data.getHouse_id());
                temp_sql.setString(2, data.getYear());
                temp_sql.setString(3, data.getMonth());
                temp_sql.setString(4, data.getDay());
                temp_sql.setInt(5, data.getWindows());
                temp_sql.setInt(6, data.getSlice_num());
                temp_sql.setDouble(7, data.getValue());
                String statementText = temp_sql.toString();
                sql += statementText.substring(statementText.indexOf(": ") + 2) + ",";
            }
            sql = sql.substring(0, sql.length() - 1) + " on duplicate key update avg=VALUES(avg)";
            stmt.executeUpdate(sql);
            conn.close();
            System.out.printf("\nDB tooks %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean pushForecastHouseData(Stack<HouseData> data_list) {
        try {
            // Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            // Init SQL
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            String sql = "insert into house_data_forecast (house_id,year,month,day,windows,slice_num,avg) values ";
            for (HouseData data : data_list) {
                PreparedStatement temp_sql = conn.prepareStatement("(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                temp_sql.setInt(1, data.getHouse_id());
                temp_sql.setString(2, data.getYear());
                temp_sql.setString(3, data.getMonth());
                temp_sql.setString(4, data.getDay());
                temp_sql.setInt(5, data.getWindows());
                temp_sql.setInt(6, data.getSlice_num());
                temp_sql.setDouble(7, data.getValue());
                String statementText = temp_sql.toString();
                sql += statementText.substring(statementText.indexOf(": ") + 2) + ",";
            }
            sql = sql.substring(0, sql.length() - 1) + " on duplicate key update avg=VALUES(avg)";
            stmt.executeUpdate(sql);
            conn.close();
            // System.out.printf("\nDB tooks %.2f
            // s\n",(float)(System.currentTimeMillis()-start)/1000);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public boolean pushForecastHouseData(HouseData data, String table) {
        try {
            // Init SQL
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            String sql = "insert into " + table + " (house_id,year,month,day,windows,slice_num,avg) values ";
            PreparedStatement temp_sql = this.conn.prepareStatement("(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            temp_sql.setInt(1, data.getHouse_id());
            temp_sql.setString(2, data.getYear());
            temp_sql.setString(3, data.getMonth());
            temp_sql.setString(4, data.getDay());
            temp_sql.setInt(5, data.getWindows());
            temp_sql.setInt(6, data.getSlice_num());
            temp_sql.setDouble(7, data.getValue());
            String statementText = temp_sql.toString();
            sql += statementText.substring(statementText.indexOf(": ") + 2) + ",";
            sql = sql.substring(0, sql.length() - 1) + " on duplicate key update avg=VALUES(avg)";
            stmt.executeUpdate(sql);
            // System.out.printf("\nDB tooks %.2f
            // s\n",(float)(System.currentTimeMillis()-start)/1000);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Trying again");
            this.reConnect();
            return pushForecastHouseData(data,table);
        }
    }

    public static Stack<String> pushDeviceData(Stack<DeviceData> data_list) {
        Stack<String> result = new Stack<String>();
        try {
            System.out.printf("\n\n\nStoring %d\n\n\n", data_list.size());
            // Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            // Init SQL
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            for (DeviceData data : data_list) {
                PreparedStatement temp_sql = conn.prepareStatement(
                        "insert into device_data (house_id,household_id,device_id,year,month,day,windows,slice_num,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), count=VALUES(count), avg=VALUES(avg)",
                        Statement.RETURN_GENERATED_KEYS);
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

    public static boolean saveData(DeviceData data) {
        try {
            // Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            // Init SQL
            Statement stmt = conn.createStatement();
            stmt.execute("use iot_data");
            PreparedStatement temp_sql = conn.prepareStatement(
                    "insert into device_data (house_id,household_id,device_id,year,month,day,windows,slice_num,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), count=VALUES(count), avg=VALUES(avg)",
                    Statement.RETURN_GENERATED_KEYS);
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

    // public static Double forecast(HouseData data){
    // try{
    // //Init connection
    // Yaml yaml = new Yaml();
    // FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
    // Map<String, Object> obj = yaml.load(inputStream);
    // String dbURL = "jdbc:mysql://"+obj.get("db_url");
    // String userName = (String) obj.get("db_user");
    // String password = (String) obj.get("db_pass");
    // Class.forName("com.mysql.jdbc.Driver");
    // Connection conn = DriverManager.getConnection(dbURL, userName, password);
    // Statement stmt = conn.createStatement();
    // stmt.execute("use iot_data");
    // } catch (Exception ex) {
    // ex.printStackTrace();
    // return (double) -1;
    // }
    // }

    public Stack<HouseData> query(int house_id, String year, String month, String day, int windows,
            int slice_num) {
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            String sql = "SELECT * FROM house_data WHERE ";
            Boolean condition = false;
            if (house_id != -1) {
                sql += "house_id=" + house_id + " AND ";
                condition = true;
            }
            if (year.length() != 0) {
                sql += "year=\"" + year + "\" AND ";
                condition = true;
            }
            if (month.length() != 0) {
                sql += "month=\"" + month + "\" AND ";
                condition = true;
            }
            if (day.length() != 0) {
                sql += "day=\"" + day + "\" AND ";
                condition = true;
            }
            if (house_id != -1) {
                sql += "windows=" + windows + " AND ";
                condition = true;
            }
            if (slice_num != -1) {
                sql += "slice_num=" + slice_num + " AND ";
                condition = true;
            }
            try(ResultSet rs = stmt.executeQuery(sql.substring(0, sql.length() - (condition ? 5 : 7)))){
                while (rs.next()) {
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_num"), rs.getInt("windows"), rs.getDouble("avg")));
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            this.reConnect();
            System.out.println("Trying again");
            return query(house_id, year, month, day, windows, slice_num);
        }
    }

    public Stack<HouseData> queryBeforeV0(int house_id, String year, String month, String day, int windows,
            int slice_num) {
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && windows < 0
                    && slice_num < 0) {
                return new Stack<>();
            }
            String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND year=\"" + year
            + "\" AND month=\"" + month + "\" AND day=\"" + day + "\" AND windows=" + windows;
            try(ResultSet rs = stmt.executeQuery(sql)){
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900, Integer.valueOf(rs.getString("month"))-1,
                            Integer.valueOf(rs.getString("day"))).after(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month"))-1, Integer.valueOf(rs.getString("day"))).equals(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        if (rs.getInt("slice_num") > slice_num) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_num"), rs.getInt("windows"), rs.getDouble("avg")));
                }
                return result;
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Trying again");
            this.reConnect();
            return queryBeforeV0(house_id, year, month, day, windows, slice_num);
        }
    }

    public Stack<HouseData> queryBeforeV1(int house_id, String year, String month, String day, int windows,
            int slice_num) {
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && windows < 0
                    && slice_num < 0) {
                return new Stack<>();
            }
            String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND windows=" + windows;
            try(ResultSet rs = stmt.executeQuery(sql)){
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900, Integer.valueOf(rs.getString("month"))-1,
                            Integer.valueOf(rs.getString("day"))).after(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month"))-1, Integer.valueOf(rs.getString("day"))).equals(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        if (rs.getInt("slice_num") > slice_num) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_num"), rs.getInt("windows"), rs.getDouble("avg")));
                }
                return result;
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Trying again");
            this.reConnect();
            return queryBeforeV1(house_id, year, month, day, windows, slice_num);
        }
    }

    public Stack<HouseData> queryBeforeV2(int house_id, String year, String month, String day, int windows,
            int slice_num) {
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Boolean end = false;
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && windows < 0
                    && slice_num < 0) {
                return new Stack<>();
            }
            Calendar query_cal = Calendar.getInstance();
            query_cal.setTime(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)));
            do{
                query_cal.add(Calendar.WEEK_OF_YEAR, -1);
                try(ResultSet rs = stmt.executeQuery("SELECT * FROM house_data WHERE house_id="+ house_id +" AND year=\"" + query_cal.get(Calendar.YEAR) + "\" AND month=\"" + String.format("%02d",(query_cal.get(Calendar.MONTH)+1)) + "\" AND day=\"" + String.format("%02d",query_cal.get(Calendar.DAY_OF_MONTH)) + "\" AND windows=" + windows + " AND slice_num="+slice_num)){
                    if(rs.next()){
                        result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                        rs.getString("day"), rs.getInt("slice_num"), rs.getInt("windows"), rs.getDouble("avg")));
                    }
                    else{
                        end=true;
                    }
                }
            }
            while(!end);
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Trying again");
            this.reConnect();
            return queryBeforeV2(house_id, year, month, day, windows, slice_num);
        }
    }

    public Stack<HouseData> queryBeforeV3(int house_id, String year, String month, String day, int windows,
            int slice_num) {
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute("use iot_data");
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && windows < 0
                    && slice_num < 0) {
                return new Stack<>();
            }
            String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND slice_num=" + slice_num +" windows=" + windows;
            try(ResultSet rs = stmt.executeQuery(sql)){
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900, Integer.valueOf(rs.getString("month"))-1,
                            Integer.valueOf(rs.getString("day"))).after(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month"))-1, Integer.valueOf(rs.getString("day"))).equals(
                                    new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month)-1, Integer.valueOf(day)))) {
                        if (rs.getInt("slice_num") > slice_num) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_num"), rs.getInt("windows"), rs.getDouble("avg")));
                }
                return result;
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Trying again");
            this.reConnect();
            return queryBeforeV3(house_id, year, month, day, windows, slice_num);
        }
    }

    public void reConnect() {
        try {
            this.close();
            // Init connection
            Yaml yaml = new Yaml();
            FileInputStream inputStream = new FileInputStream(new File("cred.yaml"));
            Map<String, Object> obj = yaml.load(inputStream);
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.jdbc.Driver");
            this.conn = DriverManager.getConnection(dbURL, userName, password);
        } catch (Exception ex) {
            System.out.println("connect failure! Retrying...");
            this.reConnect();
            ex.printStackTrace();
        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
