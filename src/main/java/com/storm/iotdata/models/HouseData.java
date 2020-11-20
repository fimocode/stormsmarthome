package com.storm.iotdata.models;

import java.io.Serializable;
import java.util.Objects;

/**
 * HouseData
 */
public class HouseData extends Timeslice implements Serializable{

    public Integer houseId;
    public Double value;
    public Double count;
    public Long lastUpdate;
    public Boolean saved = false;

    public HouseData(Integer houseId, Timeslice timeslice, Double value) {
        super(timeslice);
        this.houseId = houseId;
        this.value = value;
        this.count = Double.valueOf(1);
        this.setLastUpdate();
        this.saved = false;
    }

    public HouseData(Integer houseId, Timeslice timeslice, Double value, Double count, Boolean saved) {
        super(timeslice);
        this.houseId = houseId;
        this.value = value;
        this.count = count;
        this.setLastUpdate();
        this.saved = saved;
    }

    public HouseData(Integer houseId, String timeslice, Double value) {
        super(timeslice);
        this.houseId = houseId;
        this.value = value;
        this.setLastUpdate();
        this.saved = false;
    }

    public HouseData(Integer houseId, String timeslice, Double value, Boolean saved) {
        super(timeslice);
        this.houseId = houseId;
        this.value = value;
        this.setLastUpdate();
        this.saved = saved;
    }

    public HouseData(Integer houseId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.value = value;
        this.setLastUpdate();
        this.saved = false;
    }

    public HouseData(Integer houseId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value, Long lastUpdate, Boolean saved) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.value = value;
        this.setLastUpdate();
        this.saved = saved;
    }

    public HouseData(Integer houseId, String year, String month, String day, Integer sliceIndex, Integer sliceGap) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.value = Double.valueOf(0);
        this.setLastUpdate();
        this.saved = false;
    }

    public Integer getHouseId() {
        return this.houseId;
    }

    public void setHouseId(Integer houseId) {
        if(this.houseId != houseId){
            this.setLastUpdate();
            this.saved = false;
            this.houseId = houseId;
        }
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        if(!this.value.equals(value)){
            this.setLastUpdate();
            this.saved = false;
            this.value = value;
        }
    }

    public Double getCount() {
        return this.count;
    }

    public void setCount(Double count) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.count=count;
    }

    public Long getLastUpdate() {
        return this.lastUpdate;
    }

    public Boolean isSaved() {
        return this.saved;
    }

    public HouseData save() {
        this.saved = true;
        return this;
    }

    public HouseData increaseValue(Double value){
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.value+=value;
        this.count++;
        return this;
    }

    public HouseData count(Double count) {
        this.setCount(count);
        return this;
    }

    public HouseData houseId(Integer houseId) {
        this.setHouseId(houseId);
        return this;
    }

    public HouseData value(Double value) {
        this.setValue(value);
        return this;
    }
    
    public void setLastUpdate() {
        this.lastUpdate = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "{" +
            " houseId='" + getHouseId() + "'" +
            ", year='" + getYear() + "'" +
            ", month='" + getMonth() + "'" +
            ", day='" + getDay() + "'" +
            ", sliceIndex='" + getIndex() + "'" +
            ", sliceGap='" + getGap() + "'" +
            ", value='" + getValue() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

	public Double getAvg() {
		if(this.count==0){
            return  Double.valueOf(0);
        }
        return this.value/this.count;
	}

    public String getUniqueId() {
        return String.format("%d-%s-%s-%s-%d", houseId, year, month, day, sliceIndex);
    }

    public String getHouseUniqueId() {
        return String.valueOf(getHouseId());
    }

}