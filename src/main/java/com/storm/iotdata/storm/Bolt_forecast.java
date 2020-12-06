package com.storm.iotdata.storm;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.models.*;

public class Bolt_forecast extends BaseRichBolt {
    private StormConfig config;
    private int gap;
    private HashMap<String,HouseData> houseDataList;
    private HashMap<String,HouseholdData> householdDataList;
    private HashMap<String,DeviceData> deviceDataList;
    private OutputCollector _collector;

    public Bolt_forecast(Integer gap, StormConfig config){
        this.gap = gap;
        this.config = config;
        houseDataList = new HashMap<String,HouseData>();
        householdDataList = new HashMap<String,HouseholdData>();
        deviceDataList = new HashMap<String,DeviceData>();
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try{
            if(input.contains("trigger")){
                //Start forecast
                HashMap<String, HouseData> houseDataForecast= forecast(HouseData.class, houseDataList);
                Stack<HouseData> tempHouseDataForecast = new Stack<HouseData>();
                tempHouseDataForecast.addAll(houseDataForecast.values());
                if(DB_store.pushHouseDataForecast("v0", tempHouseDataForecast, new File("./tmp/houseDataForecast2db-" + gap + ".lck"))){
                    for(String key : houseDataForecast.keySet()){
                        houseDataList.remove(key);
                    }
                };

                HashMap<String, HouseholdData> householdDataForecast = forecast(HouseholdData.class, householdDataList);
                Stack<HouseholdData> tempHouseholdDataForecast = new Stack<HouseholdData>();
                tempHouseholdDataForecast.addAll(householdDataForecast.values());
                if(DB_store.pushHouseholdDataForecast("v0", tempHouseholdDataForecast, new File("./tmp/householdDataForecast2db-" + gap + ".lck"))){
                    for(String key : householdDataForecast.keySet()){
                        householdDataList.remove(key);
                    }
                };

                HashMap<String, DeviceData> deviceDataForecast = forecast(DeviceData.class, deviceDataList);
                Stack<DeviceData> tempDeviceDataForecast = new Stack<DeviceData>();
                tempDeviceDataForecast.addAll(deviceDataForecast.values());
                if(DB_store.pushDeviceDataForecast("v0", tempDeviceDataForecast, new File("./tmp/deviceDataForecast2db-" + gap + ".lck"))){
                    for(String key : deviceDataForecast.keySet()){
                        deviceDataList.remove(key);
                    }
                };
                _collector.ack(input);
            }
            else if(input.getValueByField("type").equals(HouseData.class)){
                HouseData data = (HouseData) input.getValueByField("data");
                houseDataList.put(data.getUniqueId(), data);
                _collector.ack(input);
            }
            else if(input.getValueByField("type").equals(HouseholdData.class)){
                HouseholdData data = (HouseholdData) input.getValueByField("data");
                householdDataList.put(data.getUniqueId(), data);
                _collector.ack(input);
            }
            else if(input.getValueByField("type").equals(DeviceData.class)){
                DeviceData data = (DeviceData) input.getValueByField("data");
                deviceDataList.put(data.getUniqueId(), data);
                _collector.ack(input);
            }
            else {
                _collector.fail(input);
            }
        } catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    public static <E> HashMap<String, E> forecast(Class E, HashMap<String,E> inputData){
        HashMap<String, E> result = new HashMap<String, E>();
        try{
            Connection conn = DB_store.initConnection();
            if(E.equals(HouseData.class)){
                for(String key : inputData.keySet()){
                    HouseData ele = (HouseData) inputData.get(key);
                    HashMap<String, HouseData> beforeData = DB_store.queryBefore(ele, conn);
                    if(beforeData.isEmpty()){
                        HouseData forecastResult = new HouseData(ele.getHouseId(), ele.getTimeslice().getNextTimeslice(2), ele.getAvg());
                        result.put(key, (E) forecastResult);
                    }
                    else{
                        ArrayList<HouseData> beforeAvgs = new ArrayList<>(beforeData.values());
                        beforeAvgs.sort(new Comparator<HouseData>(){
                            @Override
                            public int compare(HouseData data1, HouseData data2) {
                                return Double.compare(data1.getAvg(), data2.getAvg());
                            }
                        });
                        Double median = Double.valueOf(0);
                        if(beforeAvgs.size()%2==0){
                            median = (beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg() + beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1).getAvg())/2;
                        }
                        else {
                            median = beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg();
                        }
                        Double value = (ele.getAvg() + median)/2;
                        value = (ele.getAvg() + median)/2;
                        HouseData forecastResult = new HouseData(ele.getHouseId(), ele.getTimeslice().getNextTimeslice(2), value);
                        result.put(key, (E) forecastResult);
                    }
                }
            }
            else if(E.equals(HouseholdData.class)){
                for(String key : inputData.keySet()){
                    HouseholdData ele = (HouseholdData) inputData.get(key);
                    HashMap<String, HouseholdData> beforeData = DB_store.queryBefore(ele, conn);
                    if(beforeData.isEmpty()){
                        HouseholdData forecastResult = new HouseholdData(ele.getHouseId(), ele.getHouseholdId(), ele.getTimeslice().getNextTimeslice(2), ele.getAvg());
                        result.put(key, (E) forecastResult);
                    }
                    else{
                        ArrayList<HouseholdData> beforeAvgs = new ArrayList<>(beforeData.values());
                        beforeAvgs.sort(new Comparator<HouseholdData>(){
                            @Override
                            public int compare(HouseholdData data1, HouseholdData data2) {
                                return Double.compare(data1.getAvg(), data2.getAvg());
                            }
                        });
                        Double median = Double.valueOf(0);
                        if(beforeAvgs.size()%2==0){
                            median = (beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg() + beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1).getAvg())/2;
                        }
                        else {
                            median = beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg();
                        }
                        Double value = (ele.getAvg() + median)/2;
                        value = (ele.getAvg() + median)/2;
                        HouseholdData forecastResult = new HouseholdData(ele.getHouseId(), ele.getHouseholdId() , ele.getTimeslice().getNextTimeslice(2), value);
                        result.put(key, (E) forecastResult);
                    }
                }
            }
            else if(E.equals(DeviceData.class)){
                for(String key : inputData.keySet()){
                    DeviceData ele = (DeviceData) inputData.get(key);
                    HashMap<String, DeviceData> beforeData = DB_store.queryBefore(ele, conn);
                    if(beforeData.isEmpty()){
                        DeviceData forecastResult = new DeviceData(ele.getHouseId(), ele.getHouseholdId(), ele.getDeviceId(), ele.getTimeslice().getNextTimeslice(2), ele.getAvg());
                        result.put(key, (E) forecastResult);
                    }
                    else{
                        ArrayList<DeviceData> beforeAvgs = new ArrayList<>(beforeData.values());
                        beforeAvgs.sort(new Comparator<DeviceData>(){
                            @Override
                            public int compare(DeviceData data1, DeviceData data2) {
                                return Double.compare(data1.getAvg(), data2.getAvg());
                            }
                        });
                        Double median = Double.valueOf(0);
                        if(beforeAvgs.size()%2==0){
                            median = (beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg() + beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1).getAvg())/2;
                        }
                        else {
                            median = beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)).getAvg();
                        }
                        Double value = (ele.getAvg() + median)/2;
                        value = (ele.getAvg() + median)/2;
                        DeviceData forecastResult = new DeviceData(ele.getHouseId(), ele.getHouseholdId(), ele.getDeviceId(), ele.getTimeslice().getNextTimeslice(2), value);
                        result.put(key, (E) forecastResult);
                    }
                }
            }
            conn.close();
        } catch (Exception ex){
            ex.printStackTrace();
        } finally {
            return result;
        }
    }
}
