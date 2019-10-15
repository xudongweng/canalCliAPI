/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal.helper;

import com.mongodb.BasicDBObject;
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
    private String db="test";
    
    
    public MongoDBHelper(){
        this.log=Logger.getLogger(MongoDBHelper.class);
    }
    
    public void setUrlplus(String urlplus){
        this.urlplus=urlplus;
    }
    
    public void setDB(String db){
        this.db=db;
    }
    
    public void setUrl(String urlplus,String db){
        this.db=db;
        this.urlplus=urlplus;
    }
    
    public boolean dropDB(){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            mongoDatabase.drop();
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus);
        }
        return false;
    }
    
    public boolean dropDB(String db){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            mongoDatabase.drop();
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db);
        }
        return false;
    }
    
    public boolean dropTable(String table){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.drop();
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table);
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
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table);
        }
        return false;
    }

    public boolean insertDoc(String table,Document document){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            List<Document> documents = new ArrayList<>();  
            documents.add(document);  
            collection.insertMany(documents);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [document:]"+document.toJson());
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
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [document:]"+document.toJson());
        }
        return false;
    }
    
        
    public boolean insertDocs(String table,List<Document> documents){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.insertMany(documents);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [list:]"+documents.toString());
        }
        return false;
    }
    
    public boolean insertDocs(String db,String table,List<Document> documents){
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
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [list:]"+documents.toString());
        }
        return false;
    }
    
    public boolean updateOneDoc(String table,BasicDBObject updateOldSql,BasicDBObject updateNewSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.updateOne(updateOldSql, updateNewSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [updateOldSql:]"+updateOldSql+" [updateNewSql:]"+updateNewSql);
        }
        return false;
    }
    
    public boolean updateOneDoc(String db,String table,BasicDBObject updateOldSql,BasicDBObject updateNewSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.updateOne(updateOldSql, updateNewSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [updateOldSql:]"+updateOldSql+" [updateNewSql:]"+updateNewSql);
        }
        return false;
    }
    
    public boolean updateManyDoc(String table,BasicDBObject updateOldSql,BasicDBObject updateNewSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.updateMany(updateOldSql, updateNewSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [updateOldSql:]"+updateOldSql+" [updateNewSql:]"+updateNewSql);
        }
        return false;
    }
    
    public boolean updateManyDoc(String db,String table,BasicDBObject updateOldSql,BasicDBObject updateNewSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.updateMany(updateOldSql, updateNewSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [updateOldSql:]"+updateOldSql+" [updateNewSql:]"+updateNewSql);
        }
        return false;
    }
    
    public boolean deleteOneDoc(String table,BasicDBObject delSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.deleteOne(delSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [delSql:]"+delSql);
        }
        return false;
    }
    
    public boolean deleteOneDoc(String db,String table,BasicDBObject delSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.deleteOne(delSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [delSql:]"+delSql);
        }
        return false;
    }
    
    public boolean deleteManyDoc(String table,BasicDBObject delSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.deleteMany(delSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [delSql:]"+delSql);
        }
        return false;
    }
    
    public boolean deleteManyDoc(String db,String table,BasicDBObject delSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            collection.deleteMany(delSql);
            mongoClient.close();
            return true;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [delSql:]"+delSql);
        }
        return false;
    }
    
    public long findCount(String table,BasicDBObject findSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(this.db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            long i=collection.countDocuments(findSql);
            mongoClient.close();
            return i;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+this.db+" [table:]"+table+" [findSql:]"+findSql);
        }
        return -1;
    }
    
    public long findCount(String db,String table,BasicDBObject findSql){
        // 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient(new MongoClientURI(this.url+this.urlplus+"/"+db));
        try{
            //System.out.println(this.server+this.port);
            // 连接到数据库，需要有runoob数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
            MongoCollection<Document> collection =mongoDatabase.getCollection(table);
            long i=collection.countDocuments(findSql);
            mongoClient.close();
            return i;
        }catch (MongoException e) {
            log.error(e.toString()+" [urlplus:]"+this.urlplus+" [db:]"+db+" [table:]"+table+" [findSql:]"+findSql);
        }
        return -1;
    }
}
