package com.potato.poiMatch.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.potato.mobile.service.BehaviorStatService;
import com.potato.poiMatch.common.MongoDBClientInstance;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Properties;

/**
 * Created by potato on 2018/5/18.
 */
public class MongoWriteService {

    private static MongoWriteService service;

    public static MongoWriteService getInstance() {
        if (service == null) {
            synchronized (MongoWriteService.class) {
                if (service == null) {
                    service = new MongoWriteService();
                }
            }
        }
        return service;
    }
    public boolean UpDate(String db, String col, String id, Document doc) throws Exception {
        MongoCollection<Document> collection = MongoDBClientInstance.GetCollection(db,col);
        if(collection==null){
            throw new Exception("获取文档失败");
        }
        UpdateResult re = collection.updateOne(Filters.eq("_id",new ObjectId(id)),new Document("$set",doc));
        System.out.println(re.toString());
        if(re.getMatchedCount()>0){
            return  true;
        }
        return  false;
    }

    public void FormatNoMatchTable(String db, String col) throws Exception{
        MongoCollection<Document> collection = MongoDBClientInstance.GetCollection(db,col);
        if(collection==null){
            throw new Exception("获取文档失败");
        }
        //match_status 若为null，则设为零'
        Document doc = new Document("match_status",0);
        collection.updateMany((Filters.not(Filters.exists("match_status"))),new Document(new Document("$set",doc)));
        collection.updateMany((Filters.not(Filters.exists("scan_time"))),new Document(new Document("$set",new Document("scan_time",0L))));
        collection.updateMany((Filters.not(Filters.exists("match_time"))),new Document(new Document("$set",new Document("match_time",0L))));
    }


    public static void main(String[] args) {
        MongoWriteService service = MongoWriteService.getInstance();
        Document doc = new Document("who","hahhahhah");
        doc.append("firstName","Guanghui");
        try {
            //boolean result = service.UpDate("mga-prod","no_match_min","5a67f3c06b818943335e57e2",doc);
            service.FormatNoMatchTable("mga-prod","no_match_min");
            //System.out.println("插入结果："+result);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
