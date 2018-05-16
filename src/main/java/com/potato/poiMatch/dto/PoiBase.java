package com.potato.poiMatch.dto;

import lombok.Data;

import java.util.List;

/**
 * Created by potato on 2018/5/10.
 */
@Data
public class PoiBase {
    private String id;
    private String name;
    private String typecode;
    private String address;
    private Long geoNum;
    private String location;
    private List<String> tel;
    public String getLat(){
        return location.split(",")[0];
    }
    public String getLng(){
        return location.split(",")[1];
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypecode() {
        return typecode;
    }

    public void setTypecode(String typecode) {
        this.typecode = typecode;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Long getGeoNum() {
        return geoNum;
    }

    public void setGeoNum(Long geoNum) {
        this.geoNum = geoNum;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public List<String> getTel() {
        return tel;
    }

    public void setTel(List<String> tel) {
        this.tel = tel;
    }
}
