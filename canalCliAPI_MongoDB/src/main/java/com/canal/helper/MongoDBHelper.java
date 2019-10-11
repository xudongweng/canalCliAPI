/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal.helper;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bson.Document;

/**
 *
 * @author sheriff
 */
public class MongoDBHelper {
    //通过连接认证获取MongoDB连接mongodb://mongouser:thepasswordA1@localhost:27017/admin
    //副本连接方式mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet
    private final String url="mongodb://";
    private String urlplus="localhost:27017";
    private Logger log=null;
    
    public MongoDBHelper(){
        this.log=Logger.getLogger(MongoDBHelper.class);
    }
    
    public void setUrlplus(String urlplus){
        this.urlplus=urlplus;
    }
    
    public boolean createTable(String db,String table){
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            mongoDatabase.createCollection(table);
            
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
    
    public boolean dropTable(String db,String table){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.drop();
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
    
    public boolean insertDoc(String db,String table,Document document){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            List<Document> documents = new ArrayList<>();  
            documents.add(document);  
            collection.insertMany(documents);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
    
    public boolean insertDoc(String db,String table,List<Document> documents){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.insertMany(documents);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
    
    public boolean updateDoc(String db,String table,Document document){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.updateMany(document, document);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
}
