package com.potato.poiMatch.dto;

import lombok.Data;

import java.util.List;

/**hbase 请求poi返回结果结构信息
 * Created by potato on 2018/5/9.
 */
@Data
public class GeosotJiugongRe {
    private List<Long> geoNums;
    private String info;
    private Integer server_status;
    private String server_error;

}
