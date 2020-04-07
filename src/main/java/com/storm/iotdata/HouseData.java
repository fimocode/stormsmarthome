package com.storm.iotdata;

import java.io.Serializable;
import java.util.Objects;

/**
 * HouseData
 */
public class HouseData implements Serializable{

    public int house_id;
    public String year;
    public String month;
    public String day;
    public int slice_num;
    public int windows;
    public Double value;
    public Long lastUpdate;
    public boolean saved = false;

    public HouseData(int house_id, String year, String month, String day, int slice_num, int windows, Double value) {
        this.house_id = house_id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.slice_num = slice_num;
        this.windows = windows;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
    }

    public HouseData(int house_id, String year, String month, String day, int slice_num, int windows, Double value, Long lastUpdate, boolean saved) {
        this.house_id = house_id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.slice_num = slice_num;
        this.windows = windows;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = saved;
    }

    public HouseData(int house_id, String year, String month, String day, int slice_num, int windows) {
        this.house_id = house_id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.slice_num = slice_num;
        this.windows = windows;
        this.value = (double) 0;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getHouse_id() {
        return this.house_id;
    }

    public void setHouse_id(int house_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.house_id = house_id;
    }

    public String getYear() {
        return this.year;
    }

    public void setYear(String year) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.year = year;
    }

    public String getMonth() {
        return this.month;
    }

    public void setMonth(String month) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.month = month;
    }

    public String getDay() {
        return this.day;
    }

    public void setDay(String day) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.day = day;
    }

    public int getSlice_num() {
        return this.slice_num;
    }

    public void setSlice_num(int slice_num) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.slice_num = slice_num;
    }

    public int getWindows() {
        return this.windows;
    }

    public void setWindows(int windows) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.windows = windows;
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.value = value;
    }

    public Long getLastUpdate() {
        return this.lastUpdate;
    }

    public boolean isSaved() {
        return this.saved;
    }

    public HouseData saved() {
        this.saved = true;
        return this;
    }

    public HouseData house_id(int house_id) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.house_id = house_id;
        return this;
    }

    public HouseData year(String year) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.year = year;
        return this;
    }

    public HouseData month(String month) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.month = month;
        return this;
    }

    public HouseData day(String day) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.day = day;
        return this;
    }

    public HouseData slice_num(int slice_num) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.slice_num = slice_num;
        return this;
    }

    public HouseData windows(int windows) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.windows = windows;
        return this;
    }

    public HouseData value(Double value) {
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        this.value = value;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(house_id, year, month, day, slice_num, windows, value, lastUpdate, saved);
    }

    @Override
    public String toString() {
        return "{" +
            " house_id='" + getHouse_id() + "'" +
            ", year='" + getYear() + "'" +
            ", month='" + getMonth() + "'" +
            ", day='" + getDay() + "'" +
            ", slice_num='" + getSlice_num() + "'" +
            ", windows='" + getWindows() + "'" +
            ", value='" + getValue() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

    public String getSliceName(){
        return year + "/" + month + "/" + day + " " +  String.format("%02d", Math.floorDiv((slice_num*windows),60)) + ":" +  String.format("%02d", (slice_num*windows)%60) + "->" +  String.format("%02d", Math.floorDiv(((slice_num+1)*windows),60)) + ":" +  String.format("%02d", ((slice_num+1)*windows)%60) ;
    }

    // public HouseData forecast(){
    //     if(db_store.forecast(this));
    //     return this;
    // }
}