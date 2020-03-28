package com.storm.iotdata;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Stack;

/**
 * Forecast
 */
public class Forecast extends Thread{
    public int speed = 0;
    public int house_id;
    public Date begin;
    public int windows;
    public db_store conn;

    public Forecast(int house_id, Date begin, int windows){
        this.house_id = house_id;
        this.begin = begin;
        this.windows = windows;
        this.conn = new db_store();
    }

    public void forecast(int house_id, Date begin, int windows){
        System.out.println("Start forecast V1");
        boolean end = false;
        Calendar now = Calendar.getInstance();
        now.setTime(begin);
        int index = 0;
        int total_slice = (int) 24*60/windows;
        Stack<HouseData> current;
        Calendar temp;
        Stack<Double> data_list;
        while(!end){
            current= conn.query(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index);
            if(current.size()==1){
                data_list = new Stack<Double>();
                for(HouseData data : conn.queryBefore(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index)){
                    data_list.add(data.getValue());
                }
                if(data_list.size()==0){
                    continue;
                }
                if(index+2>=total_slice){
                    temp = Calendar.getInstance();
                    temp.setTime(now.getTime());
                    temp.add(Calendar.DAY_OF_YEAR, 1);
                    conn.pushForecastHouseData(new HouseData(house_id, String.format("%d", temp.get(Calendar.YEAR)), String.format("%02d", temp.get(Calendar.MONTH)+1), String.format("%02d", temp.get(Calendar.DAY_OF_MONTH)), (int)(index+2)%total_slice, windows, (Double) (current.get(0).getValue() + median(data_list))/2),"house_data_forecast_v1");
                    // System.out.println(new HouseData(house_id, String.format("%d", temp.get(Calendar.YEAR)), String.format("%02d", temp.get(Calendar.MONTH)), String.format("%02d", temp.get(Calendar.DAY_OF_MONTH)), (int)(index+2)%total_slice, windows, forecast_value));
                }
                else{
                    conn.pushForecastHouseData(new HouseData(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), index+2, windows, (Double) (current.get(0).getValue() + median(data_list))/2),"house_data_forecast_v1");
                    // System.out.println(new HouseData(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), index+2, windows, forecast_value));
                }
                if(index+1>=total_slice){
                    now.add(Calendar.DAY_OF_YEAR, 1);
                    index = (index+1)%total_slice;
                }
                else{
                    index++;
                }
            }
            else if(now.getTime().before(new Date(113,8,31))){
                System.out.println("Missing slice: " + index + " | windows: " + windows + " | date: " + now.getTime().toGMTString());
                if(index+1>=total_slice){
                    now.add(Calendar.DAY_OF_YEAR, 1);
                    index = (index+1)%total_slice;
                }
                else{
                    index++;
                }
            }
            else{
                end=true;
            }
            this.speed++;
        }
    }

    public void forecastv2(int house_id, Date begin, int windows){
        System.out.println("Start forecast V2");
        boolean end = false;
        Calendar now = Calendar.getInstance();
        now.setTime(begin);
        int index = 0;
        int total_slice = (int) 24*60/windows;
        Stack<HouseData> current;
        Stack<Double> data_list;
        Calendar temp;
        while(!end){
            current = conn.query(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index);
            if(current.size()==1){
                data_list = new Stack<Double>();
                for(HouseData data : conn.queryBeforeV2(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index)){
                    data_list.add(data.getValue());
                }
                if(data_list.size()!=0){
                    if(index+2>=total_slice){
                        temp = Calendar.getInstance();
                        temp.setTime(now.getTime());
                        temp.add(Calendar.DAY_OF_YEAR, 1);
                        conn.pushForecastHouseData(new HouseData(house_id, String.format("%d", temp.get(Calendar.YEAR)), String.format("%02d", temp.get(Calendar.MONTH)+1), String.format("%02d", temp.get(Calendar.DAY_OF_MONTH)), (int)(index+2)%total_slice, windows, (Double) (current.get(0).getValue() + median(data_list))/2),"house_data_forecast_v2");
                        // System.out.println(new HouseData(house_id, String.format("%d", temp.get(Calendar.YEAR)), String.format("%02d", temp.get(Calendar.MONTH)), String.format("%02d", temp.get(Calendar.DAY_OF_MONTH)), (int)(index+2)%total_slice, windows, (Double) (current.get(0).getValue() + median(data_list))/2));
                    }
                    else{
                        conn.pushForecastHouseData(new HouseData(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)+1), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), index+2, windows, (Double) (current.get(0).getValue() + median(data_list))/2),"house_data_forecast_v2");
                        // System.out.println(new HouseData(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), index+2, windows, (Double) (current.get(0).getValue() + median(data_list))/2));
                    }
                }
                if(index+1>=total_slice){
                    now.add(Calendar.DAY_OF_YEAR, 1);
                    index = (index+1)%total_slice;
                }
                else{
                    index++;
                }
            }
            else if(now.getTime().before(new Date(113,8,31))){
                System.out.println("Missing slice: " + index + " | windows: " + windows + " | date: " + now.getTime().toGMTString());
                if(index+1>=total_slice){
                    now.add(Calendar.DAY_OF_YEAR, 1);
                    index = (index+1)%total_slice;
                }
                else{
                    index++;
                }
            }
            else{
                end=true;
            }
            this.speed++;
        }
    }

    public static Double median(Stack<Double> data_list) {
        Collections.sort(data_list);
        // sort array
        double median;
        // get count of scores
        int totalElements = data_list.size();
        // check if total number of scores is even
        if (totalElements % 2 == 0) {
           Double sumOfMiddleElements = data_list.get(totalElements / 2) +
                                     data_list.get(totalElements / 2 - 1);
           // calculate average of middle elements
           median = ((double) sumOfMiddleElements) / 2;
        } else {
           // get the middle element
           median = (double) data_list.get(totalElements / 2);
     }
     return median;
    }

    @Override
    public void run() {
        System.out.printf("\n[Forecast] Started threads for { house_id: %d | begin: %s | windows: %d }\n", house_id, begin.toGMTString(), windows);
        forecast(house_id, begin, windows);
        forecastv2(house_id, begin, windows);
        this.conn.close();
        System.out.printf("\n[Forecast] Done{ house_id: %d | begin: %s | windows: %d }\n", house_id, begin.toGMTString(), windows);
    }
}