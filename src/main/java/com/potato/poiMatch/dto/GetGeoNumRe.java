package com.potato.poiMatch.dto;

import lombok.Data;

import java.util.List;

/**hbase 请求poi返回结果结构信息
 * Created by potato on 2018/5/9.
 */
@Data
public class GetGeoNumRe {
    private Long geoNum;
    private Integer server_status;
    private String server_error;

    public Long getGeoNum() {
        return geoNum;
    }

    public void setGeoNum(Long geoNum) {
        this.geoNum = geoNum;
    }

    public Integer getServer_status() {
        return server_status;
    }

    public void setServer_status(Integer server_status) {
        this.server_status = server_status;
    }

    public String getServer_error() {
        return server_error;
    }

    public void setServer_error(String server_error) {
        this.server_error = server_error;
    }
}
