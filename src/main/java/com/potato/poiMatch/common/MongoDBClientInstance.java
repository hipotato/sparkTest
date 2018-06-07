package com.potato.poiMatch.common;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;


/**
 * Created by potato on 2018/5/18.
 */
public class MongoDBClientInstance {
    private static Log log = LogFactory.getLog(MongoDBClientInstance.class);
    private static MongoClient mongoClient = null;

    public static MongoClient getInstance(){
        if (mongoClient == null) {
            synchronized (MongoClient.class) {
                if (mongoClient == null) {
                    mongoClient = new MongoClient("192.168.50.232" , 27017);
                }
            }
        }
        return mongoClient;
    }

    public static  MongoCollection<Document> GetCollection(String dbName, String colName){
        MongoClient client = MongoDBClientInstance.getInstance();
        System.out.println("client is locked :"+client.isLocked());

        MongoCollection<Document> col = client.getDatabase(dbName).getCollection(colName);
        System.out.println("Collection :"+col.count());
        return  col;
    }

}
