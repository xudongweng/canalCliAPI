/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal;

import com.canal.helper.MongoDBHelper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;

/**
 *
 * @author sheriff
 */
public class Test {
    public static void main(String[] args){
        MongoDBHelper m=new MongoDBHelper();
        /*
        Document document = new Document("title", "MongoDB2").  
            append("description", "database").  
            append("likes", 342).  
            append("by", "Fly");  
        
        m.insertDoc("test", "t1", document);
        */
        BasicDBObject updateOldSql = new BasicDBObject("by", "Fly");
        //updateOldSql.append("likes", 345);
        //BasicDBObject updateNewOneSql = new BasicDBObject("$set", new BasicDBObject("title", "xxxx"));
        m.findCount("t1", updateOldSql);
        
    
    }
}
