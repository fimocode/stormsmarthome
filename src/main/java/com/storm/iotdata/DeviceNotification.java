package com.storm.iotdata;

public class DeviceNotification {
    public int type;
    public int house_id;
    public int household_id;
    public int device_id;
    public String year;
    public String month;
    public String day;
    public int slice_num;
    public int windows;
    public Double min;
    public Double max;
    public Double avg;
    public Double value;
    public Long timestamp;
    public boolean saved = false;

    public DeviceNotification(int type, DeviceData data, DeviceProp data_prop){
        this.type = type;
        this.house_id = data.device_id;
        this.household_id = data.household_id;
        this.device_id = data.device_id;
        this.year = data.year;
        this.month = data.month;
        this.day = data.day;
        this.slice_num = data.slice_num;
        this.windows = data.windows;
        this.value = data.getAvg();
        this.min = data_prop.min;
        this.max = data_prop.max;
        this.avg = data_prop.avg;
        this.timestamp = System.currentTimeMillis();
    }

    public int getType() {
        return this.type;
    }

    public int getHouse_id() {
        return this.house_id;
    }

    public int getHousehold_id() {
        return this.household_id;
    }

    public int getDevice_id() {
        return this.device_id;
    }

    public String getYear() {
        return this.year;
    }

    public String getMonth() {
        return this.month;
    }

    public String getDay() {
        return this.day;
    }

    public int getSlice_num() {
        return this.slice_num;
    }

    public int getWindows() {
        return this.windows;
    }

    public Double getMin() {
        return this.min;
    }

    public Double getMax() {
        return this.max;
    }

    public Double getAvg() {
        return this.avg;
    }

    public Double getValue() {
        return this.value;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public boolean getSaved() {
        return this.saved;
    }

    public boolean isSaved() {
        return this.saved;
    }


}
