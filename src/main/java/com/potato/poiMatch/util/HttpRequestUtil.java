package com.potato.poiMatch.util;

import com.iwhere.hbase_util.TablePoiOperator;
import com.potato.poiMatch.common.PropertyStrategyWrapper;
import com.potato.poiMatch.dto.GeosotJiugongRe;
import com.potato.poiMatch.dto.GetGeoNumRe;
import com.potato.poiMatch.dto.HbaseReInfo;
import com.potato.poiMatch.dto.PoiInfo;
import net.sf.json.JsonConfig;
import net.sf.json.util.PropertySetStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by potato on 2018/5/9.
 */
public class HttpRequestUtil {
    public static HbaseReInfo getPoiByGeoNum(Long geoNum,Integer geoLevel) throws IOException {
        Map a =  new HashMap<String,String>();
        a.put("geoNum",geoNum);
        a.put("geoLevel",geoLevel);
        return HttpClient.doGet("http://dev.iwhere.com:8010/bearbao-footmark/hbaseTest/getPois",a, HbaseReInfo.class);
    }

    public static HbaseReInfo getPoiByGeoNumDirectHbase(Long geoNum,Integer geoLevel) throws IOException {

        TablePoiOperator operator = new TablePoiOperator();
        JsonConfig config = new JsonConfig();
        Map<String, Object> classMap = new HashMap<String, Object>();
        config.setClassMap(classMap);
        config.setRootClass(PoiInfo.class);
        config.setPropertySetStrategy(new PropertyStrategyWrapper(PropertySetStrategy.DEFAULT));
        net.sf.json.JSONArray pois = operator.getGridPoiLang(0, geoNum, geoLevel, null, true);
        List<PoiInfo> list2 = net.sf.json.JSONArray.toList(pois, new PoiInfo(), config);  Map a =  new HashMap<String,String>();
        HbaseReInfo re = new HbaseReInfo();
        re.setData(list2);
        return re;
    }


    public static GetGeoNumRe GetGeoNum(Double lng,Double lat,Integer geoLevel) throws IOException {
        Map a =  new HashMap<String,String>();
        a.put("lat",lat);
        a.put("lng",lng);
        a.put("geoLevel",geoLevel);
        return HttpClient.doGet("http://www.iwhere.com/geosot-basis/getGeoNum",a, GetGeoNumRe.class);
    }


    public static GeosotJiugongRe getGeosotJiugong(String location,Integer geoLevel) throws IOException {
        Map a =  new HashMap<String,String>();
        a.put("lng",location.split(",")[0]);
        a.put("lat",location.split(",")[1]);
        a.put("geoLevel",geoLevel);
        return HttpClient.doGet("http://dev.iwhere.com:8010/geosot-basis/getJiugong",a, GeosotJiugongRe.class);
    }
    public static List<PoiInfo> getJiugongPois(String location,final Integer geoLevel){
        long startTime = System.currentTimeMillis();
        List<PoiInfo> rePois = new ArrayList<>();
        try {
            GeosotJiugongRe jiuGong = getGeosotJiugong(location,geoLevel);
            if(jiuGong.getServer_status()!=200){
                throw new Exception("geosot服务异常：status:"+jiuGong.getServer_status()+" server_error: "+jiuGong.getServer_error());
            }
            long step1Time = System.currentTimeMillis();
            System.out.println("location:"+location+",GeoSot请求时间："+(step1Time-startTime));
            //多线程获取hbase poi
            //ExecutorService executor = Executors.newCachedThreadPool();

            List<Long> geoNums = jiuGong.getGeoNums();
            List< Future<HbaseReInfo>> futureSet =new ArrayList<>();

            for(final Long geoNum:geoNums){
                Future<HbaseReInfo> future = ThreadPoolUtils.getThreadPool().submit(new Callable<HbaseReInfo>() {
                    @Override
                    public HbaseReInfo call() throws Exception {
                        return  getPoiByGeoNum(geoNum,geoLevel);
                    }
                });
                futureSet.add(future);
            }
            for(Future<HbaseReInfo> future:futureSet){
                HbaseReInfo re =future.get(10, TimeUnit.SECONDS);
                if(re.getServer_status()!=200){
                    throw new Exception("hbase服务异常");
                }
                if(re!=null&&re.getData()!=null){
                    rePois.addAll(re.getData());
                }
            }
            System.out.println("location:"+location+",hbase请求时间："+(System.currentTimeMillis()-step1Time));

        } catch (IOException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return rePois;
        }
    }
}
