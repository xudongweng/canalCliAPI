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
import java.sql.Date;
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
    private BasicDBObject query1 = new BasicDBObject();
    private BasicDBObject query2 = new BasicDBObject();
    private boolean isDuplicate=true;//判断由于中断导致insert的重复数据是否已经结束
    private StringBuilder sb=new StringBuilder();
    
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
        this.logtrace=Boolean.valueOf(this.rb.getString("enable.bintrace"));
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
        this.logtrace=Boolean.valueOf(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance){
        this.log=Logger.getLogger(CanalAPI.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, "", "");
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.valueOf(this.rb.getString("enable.bintrace"));
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
        this.logtrace=Boolean.valueOf(this.rb.getString("enable.bintrace"));
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
                        if(rowChange.getSql().replaceAll(" ", "").toLowerCase().indexOf("droptable")==0 ||
                            rowChange.getSql().replaceAll(" ", "").toLowerCase().indexOf("truncate")==0){
                            this.isExecute=this.mongocon.dropTable(entry.getHeader().getSchemaName().replaceAll("`", ""), entry.getHeader().getTableName().replaceAll("`", ""));
                        }
                    }else if(rowChange.getSql().replaceAll(" ", "").toLowerCase().indexOf("dropdatabase")==0){
                        this.isExecute=this.mongocon.dropDB(entry.getHeader().getSchemaName().replaceAll("`", ""));
                    }
                }
                else{
                    String tableName = entry.getHeader().getTableName().replaceAll("`", "");
                    this.insertnum=0;//判断在同一批insert数据是否到了最后一条
                    for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                        switch(eventType){
                            case DELETE:
                                this.deleteColumnList(rowData.getBeforeColumnsList(),tableName);
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
        for (CanalEntry.Column column : columns) {
            sb.append(column.getMysqlType().toUpperCase());
            if(sb.indexOf("CHAR")>=0)
                this.query1.append(column.getName(), column.getValue());
            else if(sb.indexOf("INT")>=0|| sb.indexOf("YEAR")>=0){
                if(sb.indexOf("BIGINT")>=0)
                    this.query1.append(column.getName(), Long.valueOf(column.getValue()));
                else
                    this.query1.append(column.getName(), Integer.valueOf(column.getValue()));
            }else if(sb.indexOf("DATE")>=0||sb.indexOf("TIME")>=0){
                this.query1.append(column.getName(), Date.valueOf(column.getValue()));
            }else if(sb.indexOf("FLOAT")>=0||sb.indexOf("DOUBLE")>0||sb.indexOf("DECIMAL")>=0){
                this.query1.append(column.getName(), Double.valueOf(column.getValue()));
            }else
                this.query1.append(column.getName(), column.getValue());
            sb.delete(0, sb.length());
        }
        this.isExecute=this.mongocon.deleteManyDoc(tableName, this.query1);
        this.query1.clear();
    }
    
    private void insertColumnList(List<CanalEntry.Column> columns,String tableName,int rows){
        Document doc=new Document();
        for (CanalEntry.Column column : columns) {
            sb.append(column.getMysqlType().toUpperCase());
            if(sb.indexOf("CHAR")>=0){
                doc.append(column.getName(), column.getValue());
                this.query1.append(column.getName(), column.getValue());
            }else if(sb.indexOf("INT")>=0|| sb.indexOf("YEAR")>=0){
                if(sb.indexOf("BIGINT")>=0){
                    doc.append(column.getName(), Long.valueOf(column.getValue()));
                    this.query1.append(column.getName(), Long.valueOf(column.getValue()));
                }else{
                    doc.append(column.getName(), Integer.valueOf(column.getValue()));
                    this.query1.append(column.getName(), Integer.valueOf(column.getValue()));
                }
            }else if(sb.indexOf("DATE")>=0||sb.indexOf("TIME")>=0){
                doc.append(column.getName(), Date.valueOf(column.getValue()));
                this.query1.append(column.getName(), Date.valueOf(column.getValue()));
            }else if(sb.indexOf("FLOAT")>=0||sb.indexOf("DOUBLE")>0||sb.indexOf("DECIMAL")>=0){
                doc.append(column.getName(), Double.valueOf(column.getValue()));
                this.query1.append(column.getName(), Double.valueOf(column.getValue()));
            }else{
                doc.append(column.getName(), column.getValue());
                this.query1.append(column.getName(), column.getValue());
            }
            sb.delete(0, sb.length());
        }
        if(this.isDuplicate){
            if(this.mongocon.findCount(tableName, this.query1)==0){
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
        this.query1.clear();
    }
    
    private void updateColumnList(List<CanalEntry.Column> beforecos,List<CanalEntry.Column> aftercols,String tableName){
        for (CanalEntry.Column column : beforecos) {
            sb.append(column.getMysqlType().toUpperCase());
            if(sb.indexOf("CHAR")>=0)
                this.query1.append(column.getName(), column.getValue());
            else if(sb.indexOf("INT")>=0|| sb.indexOf("YEAR")>=0){
                if(sb.indexOf("BIGINT")>=0)
                    this.query1.append(column.getName(), Long.valueOf(column.getValue()));
                else
                    this.query1.append(column.getName(), Integer.valueOf(column.getValue()));
            }else if(sb.indexOf("DATE")>=0||sb.indexOf("TIME")>=0){
                this.query1.append(column.getName(), Date.valueOf(column.getValue()));
            }else if(sb.indexOf("FLOAT")>=0||sb.indexOf("DOUBLE")>=0||sb.indexOf("DECIMAL")>=0)
                this.query1.append(column.getName(), Double.valueOf(column.getValue()));
            else
                this.query1.append(column.getName(), column.getValue());
            sb.delete(0, sb.length());
        }

        for (CanalEntry.Column column : aftercols) {
            sb.append(column.getMysqlType().toUpperCase());
            if(sb.indexOf("CHAR")>=0)
                this.query2.append(column.getName(), column.getValue());
            else if(sb.indexOf("INT")>=0|| sb.indexOf("YEAR")>=0){
                if(sb.indexOf("BIGINT")>=0)
                    this.query2.append(column.getName(), Long.valueOf(column.getValue()));
                else
                    this.query2.append(column.getName(), Integer.valueOf(column.getValue()));
            }else if(sb.indexOf("DATE")>=0||sb.indexOf("TIME")>=0){
                this.query2.append(column.getName(), Date.valueOf(column.getValue()));
            }else if(sb.indexOf("FLOAT")>=0||sb.indexOf("DOUBLE")>=0||sb.indexOf("DECIMAL")>=0)
                this.query2.append(column.getName(), Double.valueOf(column.getValue()));
            else
                this.query2.append(column.getName(), column.getValue());
            sb.delete(0, sb.length());
        }
        this.mongocon.updateOneDoc(tableName, query1, query2);
        this.query1.clear();
        this.query2.clear();
    }
}
