/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal.mongodb;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.canal.helper.MongoDBHelper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.BasicDBObject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import org.bson.Document;

/**
 *
 * @author sheriff
 */
public class CanalAPI {
    private CanalConnector connector;
    private int batchSize = 1000;
    private Logger log=null;
    private Locale myLocale = null;
    private MongoDBHelper mongocon=new MongoDBHelper();
    private ResourceBundle rb=null;
    private int insertnum=0;
    private boolean isExecute=true;//sql是否执行
    private boolean isEmpty=true;//判断是否有bin日志写入
    private boolean logtrace=true;//bin日志追踪
    private List<Document> documents = new ArrayList<>();
    private BasicDBObject query = new BasicDBObject();
    private boolean isDuplicate=true;//判断由于中断导致insert的重复数据是否已经结束
    
    public boolean getIsEmpty(){
        return this.isEmpty;
    }
    
    public boolean getIsExecute(){
        return this.isExecute;
    }
    
    public void setconnect(String source, int canalport,String instance,String canaluser,String canalpassword){
        this.log=Logger.getLogger(CanalAPI.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, canaluser, canalpassword);
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance,String canaluser,String canalpassword,int batchSize){
        this.log=Logger.getLogger(CanalAPI.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, canaluser, canalpassword);
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        this.batchSize=batchSize;
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance){
        this.log=Logger.getLogger(CanalAPI.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, "", "");
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance,int batchSize){
        this.log=Logger.getLogger(CanalAPI.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, "", "");
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        this.batchSize=batchSize;
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setBatchSize(int batchSize){
        this.batchSize=batchSize;
    }
    
    public void disconnect(){
        connector.disconnect();
    }
    
    public void tansferEntry(){
        Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
        long batchId = message.getId();// 数据批号
        int size = message.getEntries().size();// 获取该批次数据的数量
        if (batchId != -1 && size != 0) {
            
            this.isExecute=true;
            this.isEmpty=false;//在有日志数据读入的情况下，不做等待处理
            List<CanalEntry.Entry> entrys=message.getEntries();
            
            for (CanalEntry.Entry entry : entrys) {
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (InvalidProtocolBufferException e) {
                    log.error(e.toString());
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                if(this.logtrace==true){
                    log.info(String.format("================BIN; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                                                 entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                                                 entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                                                 eventType));
                    log.info(String.format("================SQL:"+rowChange.getSql()));
                }
                this.mongocon.setUrl(this.rb.getString("destination.mongodb.urlplus"), entry.getHeader().getSchemaName());//设置目标数据库url
                if(!rowChange.getSql().equals(""))
                {
                    if(!entry.getHeader().getTableName().equals("")){//判断是否包含表名，不包含表名，则有可能只是对数据库的操作
                        if(rowChange.getSql().trim().toLowerCase().indexOf("droptable")==0 ||
                            rowChange.getSql().trim().toLowerCase().indexOf("truncate")==0){
                            this.mongocon.dropTable(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                        }
                    }else if(rowChange.getSql().trim().toLowerCase().indexOf("dropdatabase")==0){
                        this.mongocon.dropDB(entry.getHeader().getSchemaName());
                    }
                }
                else{
                    String tableName = entry.getHeader().getTableName();
                    this.insertnum=0;//判断在同一批insert数据是否到了最后一条
                    for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                        switch(eventType){
                            case DELETE:
                                this.deleteColumnList(rowData.getBeforeColumnsList(),entry.getHeader().getTableName());
                                break;
                            case INSERT:
                                this.insertnum++;
                                this.insertColumnList(rowData.getAfterColumnsList(),tableName,rowChange.getRowDatasList().size());
                                break;
                            default:
                                this.updateColumnList(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList(),tableName);
                                break;
                        }
                    }
                }
            }
        }else
            this.isEmpty=true;
        
        if(this.isExecute)
            connector.ack(batchId); // 提交确认
        else
            connector.rollback(batchId); // 处理失败, 回滚数据
    }
    
    private void deleteColumnList(List<CanalEntry.Column> columns,String tableName){
        BasicDBObject delSql = new BasicDBObject();
        for (CanalEntry.Column column : columns) {
            if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                    column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                    column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                delSql.append(column.getName(), column.getValue());
            else
                delSql.append(column.getName(), column.getValue());
        }
        this.isExecute=this.mongocon.deleteManyDoc(tableName, delSql);
    }
    
    private void insertColumnList(List<CanalEntry.Column> columns,String tableName,int rows){
        Document doc=new Document();
        for (CanalEntry.Column column : columns) {
            if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                    column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                    column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR")){
                doc.append(column.getName(), String.valueOf(column.getValue()));
                this.query.append(column.getName(), String.valueOf(column.getValue()));
            }
            else{
                doc.append(column.getName(), column.getValue());
                this.query.append(column.getName(), column.getValue());
            }
        }
        if(this.isDuplicate){
            if(this.mongocon.findCount(tableName, this.query)==0){
                this.documents.add(doc);
                this.isDuplicate=false;
            }
        }
        else
            this.documents.add(doc);
        
        if(this.insertnum==rows){
            this.isExecute=this.mongocon.insertDocs(tableName, documents);
            this.documents.clear();
        }
        this.query.clear();
    }
    
    private void updateColumnList(List<CanalEntry.Column> beforecos,List<CanalEntry.Column> aftercols,String tableName){
        int i=0;
        sb1.append(" WHERE ");
        for (CanalEntry.Column column : beforecos) {
            i++;
            if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                    column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                    column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                sb1.append(column.getName()).append("='").append(column.getValue()).append("'");
            else
                sb1.append(column.getName()).append("=").append(column.getValue());
            if(i<beforecos.size())
                sb1.append(" AND ");
        }
        i=0;
        sb2.append("UPDATE ").append(tableName).append(" SET ");
        for (CanalEntry.Column column : aftercols) {
            i++;
            if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                    column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                    column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                sb2.append(column.getName()).append("='").append(column.getValue()).append("'");
            else
                sb2.append(column.getName()).append("=").append(column.getValue());
            if(i<beforecos.size())
                sb2.append(",");
        }
        sb2.append(sb1);
        //System.out.println(sb2.toString());
        sb1.delete(0, sb1.length());
        sb2.delete(0, sb2.length());
    }
}
