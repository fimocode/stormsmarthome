package com.storm.iotdata;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Stack;

/**
 * Forecast
 */
public class Forecast extends Thread{
    public int house_id;
    public Date begin;
    public int windows;

    public Forecast(int house_id, Date begin, int windows){
        this.house_id = house_id;
        this.begin = begin;
        this.windows = windows;
    }

    public static void forecast(int house_id, Date begin, int windows){
        boolean end = false;
        Calendar now = Calendar.getInstance();
        now.setTime(begin);
        int index = 0;
        int total_slice = (int) 24*60/windows;
        while(!end){
            Stack<HouseData> current = db_store.query(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index);
            if(current.size()==1){
                Double current_avg = current.get(0).getValue();
                Stack<Double> data_list = new Stack<Double>();
                for(HouseData data : db_store.queryBefore(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), windows, index)){
                    data_list.add(data.getValue());
                }
                if(data_list.size()==0){
                    continue;
                }
                Double forecast_value = (Double) (current_avg + median(data_list))/2;
                Stack<HouseData> forecasted = new Stack<HouseData>();
                if(index+2>=total_slice){
                    Calendar temp = Calendar.getInstance();
                    temp.setTime(now.getTime());
                    temp.add(Calendar.DAY_OF_YEAR, 1);
                    forecasted.push(new HouseData(house_id, String.format("%d", temp.get(Calendar.YEAR)), String.format("%02d", temp.get(Calendar.MONTH)), String.format("%02d", temp.get(Calendar.DAY_OF_MONTH)), (int)(index+2)%total_slice, windows, forecast_value));
                }
                else{
                    forecasted.push(new HouseData(house_id, String.format("%d", now.get(Calendar.YEAR)), String.format("%02d", now.get(Calendar.MONTH)), String.format("%02d", now.get(Calendar.DAY_OF_MONTH)), index+2, windows, forecast_value));
                }
                // System.out.println(forecasted);
                db_store.pushForecastHouseData(forecasted);
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
        System.out.printf("\n[Forecast] Done{ house_id: %d | begin: %s | windows: %d }\n", house_id, begin.toGMTString(), windows);
    }
}