package com.potato.mobile.service;

import com.potato.common.HBaseClient;
import com.potato.utils.DateUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Properties;

public class BehaviorStatService
{
  private Properties props;
  private static BehaviorStatService service;

  public static BehaviorStatService getInstance(Properties props) {
    if (service == null) {
      synchronized (BehaviorStatService.class) {
        if (service == null) {
          service = new BehaviorStatService();
          service.props = props;
        }
      }
    }

    return service;
  }

  /*
  * 时长统计
  * */
  public void addTimeLen(String userId,String hour,String packageName,Long timeLen) {

    addUserBehaviorList(userId,hour,packageName,timeLen);
    addUserHourTimeLen(userId,hour,packageName,timeLen);
//    addUserDayTimeLen(model);
//    addUserPackageHourTimeLen(model);
//    addUserPackageDayTimeLen(model);
  }

  /*
  * 用户使用过哪些APP和使用时长
  * */
  public void addUserBehaviorList(String userId,String hour,String packageName,Long timeLen) {
    String tableName = "behavior_user_app_" + DateUtils.getMonthByHour(hour);
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = userId+":"+DateUtils.getDayByHour(hour);

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(packageName), timeLen);
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

  /*
  * 用户每小时的使用应用的时长
  * */
  public void addUserHourTimeLen(String userId,String hour,String packageName,Long timeLen) {
    String tableName = "behavior_user_hour_time_" + DateUtils.getMonthByHour(hour);
    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
    String rowKey = userId+":"+DateUtils.getDayByHour(hour);

    try {
      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyHourByHour(hour)), timeLen);
    } catch (Exception ex) {
      HBaseClient.closeTable(table);
      ex.printStackTrace();
    }
  }

//  /*
//  * 用户每天的玩机时长
//  * */
//  public void addUserDayTimeLen(UserBehaviorStatModel model) {
//    String tableName = "behavior_user_day_time_" + DateUtils.getMonthByHour(model.getHour());
//    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
//    String rowKey = String.valueOf(model.getUserId());
//
//    try {
//      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyDayByHour(model.getHour())), model.getTimeLen());
//    } catch (Exception ex) {
//      HBaseClient.closeTable(table);
//      ex.printStackTrace();
//    }
//  }
//
//  /*
//  * 用户每个应用每小时的玩机时长
//  * */
//  public void addUserPackageHourTimeLen(UserBehaviorStatModel model) {
//    String tableName = "behavior_user_hour_app_time_" + DateUtils.getMonthByHour(model.getHour());
//    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
//    String rowKey = model.getUserId()+":"+DateUtils.getDayByHour(model.getHour())+":"+model.getPackageName();
//
//    try {
//      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyHourByHour(model.getHour())), model.getTimeLen());
//    } catch (Exception ex) {
//      HBaseClient.closeTable(table);
//      ex.printStackTrace();
//    }
//  }
//
//  /*
//  * 用户每个应用每天的玩机时长
//  * */
//  public void addUserPackageDayTimeLen(UserBehaviorStatModel model) {
//    String tableName = "behavior_user_day_app_time_" + DateUtils.getMonthByHour(model.getHour());
//    Table table = HBaseClient.getInstance(this.props).getTable(tableName);
//    String rowKey = model.getUserId()+":"+model.getPackageName();
//
//    try {
//      table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("timeLen"), Bytes.toBytes(DateUtils.getOnlyDayByHour(model.getHour())), model.getTimeLen());
//    } catch (Exception ex) {
//      HBaseClient.closeTable(table);
//      ex.printStackTrace();
//    }
//  }
}
