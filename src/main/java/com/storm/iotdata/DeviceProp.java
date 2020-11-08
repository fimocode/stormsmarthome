package com.storm.iotdata;

public class DeviceProp {
    public int house_id;
    public int household_id;
    public int device_id;
    public int windows;
    public Double min;
    public Double max;
    public Double avg;
    public Long count;
    public Long lastUpdate;
    public boolean saved = false;

    public DeviceProp(int house_id, int household_id, int device_id, int windows, Double min, Double max, Long count, Double avg) {
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id;
        this.windows = windows;
        this.min = min;
        this.max = max;
        this.count = count;
        this.avg = avg;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public DeviceProp(int house_id, int household_id, int device_id, int windows, Double min, Double max, Double avg) {
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id;
        this.windows = windows;
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.count = Long.valueOf(1);
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getHouse_id() {
        return this.house_id;
    }

    public void setHouse_id(int house_id) {
        this.house_id = house_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getHousehold_id() {
        return this.household_id;
    }

    public void setHousehold_id(int household_id) {
        this.household_id = household_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getDevice_id() {
        return this.device_id;
    }

    public void setDevice_id(int device_id) {
        this.device_id = device_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getWindows() {
        return this.device_id;
    }

    public void setWindows(int windows) {
        this.windows = windows;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getMin() {
        return this.min;
    }

    public void setMin(Double min) {
        this.min = min;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getMax() {
        return this.max;
    }

    public void setMax(Double max) {
        this.max = max;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getAvg() {
        return this.avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Long getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public boolean isSaved() {
        return this.saved;
    }

    public void save() {
        this.saved = true;
    }

    public DeviceProp house_id(int house_id) {
        this.house_id = house_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp household_id(int household_id) {
        this.household_id = household_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp device_id(int device_id) {
        this.device_id = device_id;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp windows(int windows) {
        this.windows = windows;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp min(Double min) {
        this.min = min;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp max(Double max) {
        this.max = max;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp avg(Double avg) {
        this.avg = avg;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public DeviceProp addValue(Double value) {
        if(value != 0){
            this.avg = (this.avg*this.count + value)/(++this.count);
            if(value<this.getMin()){
                this.setMin(value);
            }
            else if(value>this.getMax()){
                this.setMax(value);
            }
        }
        return this;
    }

    public DeviceProp lastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
        return this;
    }

    public DeviceProp saved(boolean saved) {
        this.saved = saved;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
            " house_id='" + getHouse_id() + "'" +
            ", household_id='" + getHousehold_id() + "'" +
            ", device_id='" + getDevice_id() + "'" +
            ", min='" + getMin() + "'" +
            ", max='" + getMax() + "'" +
            ", avg='" + getAvg() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

    public String getUniqueID(){
        return String.format("%d_%d_%d", house_id, household_id, device_id);
    }
}