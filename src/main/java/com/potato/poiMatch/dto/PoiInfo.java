package com.potato.poiMatch.dto;



import lombok.Data;


/**
 * Created by potato on 2018/5/9.
 */
@Data
public class PoiInfo {
    private PoiBase base;
    public PoiBase getBase(){
        return  base;
    }
}

