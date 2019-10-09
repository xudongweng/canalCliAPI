/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal.mysql;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.canal.helper.MySQLHelper;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

/**
 *
 * @author sheriff
 */
public class CanalClient {
    private CanalConnector connector;
    private int batchSize = 1000;
    private Logger log=null;
    private MySQLHelper mysqlcon=new MySQLHelper();
    private Locale myLocale = null;
    private ResourceBundle rb=null;
    private StringBuilder sb1=new StringBuilder();
    private StringBuilder sb2=new StringBuilder();
    private int insertnum=0;
    private boolean isEmpty=true;//判断是否有bin日志写入
    private boolean isExecute=true;//sql是否执行
    private boolean logtrace=true;//bin日志追踪
    
    public boolean getIsEmpty(){
        return this.isEmpty;
    }
    
    public boolean getIsExecute(){
        return this.isExecute;
    }
    
    public void setconnect(String source, int canalport,String instance,String canaluser,String canalpassword){
        this.log=Logger.getLogger(CanalClient.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, canaluser, canalpassword);
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance,String canaluser,String canalpassword,int batchSize){
        this.log=Logger.getLogger(CanalClient.class);
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
        this.log=Logger.getLogger(CanalClient.class);
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(source,canalport), instance, "", "");
        connector.connect();
        //connector.subscribe(".*\\..*");
        connector.rollback();
        myLocale = Locale.getDefault(Locale.Category.FORMAT);
        this.rb= ResourceBundle.getBundle("config",myLocale);
        this.logtrace=Boolean.getBoolean(this.rb.getString("enable.bintrace"));
    }
    
    public void setconnect(String source, int canalport,String instance,int batchSize){
        this.log=Logger.getLogger(CanalClient.class);
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
                if(this.logtrace){
                    log.info(String.format("================; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                                                 entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                                                 entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                                                 eventType));
                    log.info(String.format("================SQL:"+rowChange.getSql()));
                }
                //设置目标数据库url
                if(!entry.getHeader().getTableName().equals(""))//判断是否包含表名，不包含表名，则有可能只是对数据库的操作
                    this.mysqlcon.setURL(this.rb.getString("destination.mysql.server")
                        , this.rb.getString("destination.mysql.port")
                        , this.rb.getString("destination.mysql.user")
                        , this.rb.getString("destination.mysql.password")
                        , entry.getHeader().getSchemaName());
                else
                    this.mysqlcon.setURL(this.rb.getString("destination.mysql.server")
                        , this.rb.getString("destination.mysql.port")
                        , this.rb.getString("destination.mysql.user")
                        , this.rb.getString("destination.mysql.password"));
                
                if(!rowChange.getSql().equals("")){
                    //System.out.println(rowChange.getSql());
                    this.isExecute=this.mysqlcon.executeSQL(rowChange.getSql());
                    if(!this.isExecute){
                        log.info(String.format("ERROR SQL EXECUTE:"+rowChange.getSql()));
                        break;
                    }
                }
                else{
                    String tableName = entry.getHeader().getTableName();
                    insertnum=0;
                    for (RowData rowData : rowChange.getRowDatasList()) {
                        switch(eventType){
                            case DELETE:
                                this.deleteColumnList(rowData.getBeforeColumnsList(),tableName);
                                break;
                            case INSERT:
                                insertnum++;
                                this.insertColumnList(rowData.getAfterColumnsList(),tableName,rowChange.getRowDatasList().size());
                                break;
                            default:
                                this.updateColumnList(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList(),tableName);
                                break;
                        }
                    }
                    if(!this.isExecute)
                        break;
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
        int i=0;
        sb1.append("DELETE FROM ").append(tableName).append(" WHERE ");
        for (CanalEntry.Column column : columns) {
            i++;
            if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                    column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                    column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                sb1.append(column.getName()).append("='").append(column.getValue()).append("'");
            else
                sb1.append(column.getName()).append("=").append(column.getValue());
            if(i<columns.size())
                sb1.append(" AND ");
        }
        //System.out.println(sb1.toString());
        this.isExecute=this.mysqlcon.executeSQL(sb1.toString());
        sb1.delete(0, sb1.length());
    }
    
    private void insertColumnList(List<CanalEntry.Column> columns,String tableName,int rows){
        int i=0;
        if(insertnum==1){
            sb1.append("REPLACE INTO ").append(tableName).append("(");
            sb2.append("VALUES(");
        }else
            sb1.append(",(");
        
        for (CanalEntry.Column column : columns) {
            i++;
            if(insertnum==1){
                sb1.append(column.getName());
                if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                        column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                        column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                    sb2.append("'").append(column.getValue()).append("'");
                else
                    sb2.append(column.getValue());

                if(i<columns.size()){
                    sb1.append(",");
                    sb2.append(",");
                }else{
                    sb1.append(") ");
                    sb2.append(")");
                }
            }else{
                if(column.getMysqlType().toUpperCase().contains("CHAR")|| column.getMysqlType().toUpperCase().contains("BLOB")||
                        column.getMysqlType().toUpperCase().contains("TEXT")||column.getMysqlType().toUpperCase().contains("TIME")||
                        column.getMysqlType().toUpperCase().contains("DATE")||column.getMysqlType().toUpperCase().contains("YEAR"))
                    sb1.append("'").append(column.getValue()).append("'");
                else
                    sb1.append(column.getValue());
                
                if(i<columns.size()){
                    sb1.append(",");
                }else{
                    sb1.append(")");
                }
            }
        }
        if(insertnum==1)
            sb1.append(sb2);
        //System.out.println(sb1.toString());
        if(insertnum==rows){
            this.isExecute=this.mysqlcon.executeSQL(sb1.toString());
            if(!this.isExecute)
                log.info(String.format("ERROR SQL EXECUTE:"+sb1.toString()));
            sb1.delete(0, sb1.length());
            sb2.delete(0, sb2.length());
        }
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
        this.isExecute=this.mysqlcon.executeSQL(sb2.toString());
        if(!this.isExecute)
                log.info(String.format("ERROR SQL EXECUTE:"+sb2.toString()));
        sb1.delete(0, sb1.length());
        sb2.delete(0, sb2.length());
    }
}
