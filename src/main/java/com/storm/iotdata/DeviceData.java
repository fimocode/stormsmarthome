package com.storm.iotdata;

import java.io.Serializable;
import java.util.Objects;

/**
 * DeviceData
 */

public class DeviceData implements Serializable{

    public int house_id;
    public int household_id;
    public int device_id;
    public String year;
    public String month;
    public String day;
    public int slice_num;
    public int windows;
    public Double value;
    public Double count;
    public Double avg;
    public Long lastUpdate;
    public boolean saved = false;

    public DeviceData(int house_id, int household_id, int device_id, String year, String month, String day, int slice_num, int windows, Double value, Double count, Boolean saved) {
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.slice_num = slice_num;
        this.windows = windows;
        this.value = value;
        this.count = count;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = saved;
    }

    public DeviceData(int house_id, int household_id, int device_id, String year, String month, String day, int slice_num, int windows) {
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.slice_num = slice_num;
        this.windows = windows;
        this.value = (double) 0;
        this.count = (double) 0;
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
    }

    public String getUniqueID(){
        return String.format("%d_%d_%d_%s_%s_%s_%d", house_id, household_id, device_id, year, month, day, slice_num);
    }

    public int getHouse_id() {
        return this.house_id;
    }

    public void setHouse_id(int house_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.house_id = house_id;
    }

    public int getHousehold_id() {
        return this.household_id;
    }

    public void setHousehold_id(int household_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.household_id = household_id;
    }

    public int getDevice_id() {
        return this.device_id;
    }

    public void setDevice_id(int device_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.device_id = device_id;
    }

    public String getYear() {
        return this.year;
    }

    public void setYear(String year) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.year = year;
    }

    public DeviceData avg(Double avg) {
        this.lastUpdate = System.currentTimeMillis();
        this.avg = avg;
        return this;
    }

    public String getMonth() {
        return this.month;
    }

    public void setMonth(String month) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.month = month;
    }

    public String getDay() {
        return this.day;
    }

    public void setDay(String day) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.day = day;
    }

    public int getSlice_num() {
        return this.slice_num;
    }

    public void setSlice_num(int slice_num) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.slice_num = slice_num;
    }

    public int getWindows() {
        return this.windows;
    }

    public void setWindows(int windows) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.windows = windows;
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.value = value;
    }

    public Double getCount() {
        return this.count;
    }

    public void setCount(Double count) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.count = count;
    }

    public long getLastUpdate() {
        return this.lastUpdate;
    }

    public Double getAvg() {
        if(this.count==0){
            return (double) 0;
        }
        return this.value/this.count;
    }

    public DeviceData house_id(int house_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.house_id = house_id;
        return this;
    }

    public DeviceData household_id(int household_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.household_id = household_id;
        return this;
    }

    public DeviceData device_id(int device_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.device_id = device_id;
        return this;
    }

    public DeviceData year(String year) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.year = year;
        return this;
    }

    public DeviceData month(String month) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.month = month;
        return this;
    }

    public DeviceData day(String day) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.day = day;
        return this;
    }

    public DeviceData saved(boolean saved) {
        this.saved = saved;
        return this;
    }

    public boolean isSaved() {
        return this.saved;
    }

    public DeviceData saved() {
        this.saved = true;
        return this;
    }

    public DeviceData slice_num(int slice_num) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.slice_num = slice_num;
        return this;
    }

    public DeviceData windows(int windows) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.windows = windows;
        return this;
    }

    public DeviceData value(Double value) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.value = value;
        return this;
    }

    public DeviceData increaseValue(Double value){
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.value+=value;
        this.count++;
        return this;
    }

    public DeviceData count(Double count) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved=false;
        this.count = count;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(house_id, household_id, device_id, year, month, day, slice_num, windows, value, count, lastUpdate);
    }

    @Override
    public String toString() {
        return "{" +
            " house_id='" + getHouse_id() + "'" +
            ", household_id='" + getHousehold_id() + "'" +
            ", device_id='" + getDevice_id() + "'" +
            ", year='" + getYear() + "'" +
            ", month='" + getMonth() + "'" +
            ", day='" + getDay() + "'" +
            ", slice_num='" + getSlice_num() + "'" +
            ", windows='" + getWindows() + "'" +
            ", value='" + getValue() + "'" +
            ", count='" + getCount() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }
    
}