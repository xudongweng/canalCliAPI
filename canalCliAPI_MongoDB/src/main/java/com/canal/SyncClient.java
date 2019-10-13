/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal;

import com.canal.helper.MongoDBHelper;
import org.bson.Document;

/**
 *
 * @author sheriff
 */
public class SyncClient {
    public static void main(String[] args){
        Document document = new Document("title", "MongoDB").  
            append("description", "database").  
            append("likes", 101).  
            append("by", "Fly");  
        MongoDBHelper m=new MongoDBHelper();
        m.insertDoc("test", "t1", document);
    }
}
