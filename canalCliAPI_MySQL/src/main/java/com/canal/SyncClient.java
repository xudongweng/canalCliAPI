package com.canal;


import com.canal.helper.MailHelper;
import com.canal.mysql.CanalAPI;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sheriff
 */
public class SyncClient {
    public static void main(String[] args){
        Locale myLocale = Locale.getDefault(Locale.Category.FORMAT);
        ResourceBundle rb= ResourceBundle.getBundle("config",myLocale);
        
        CanalAPI cc=new CanalAPI();
        cc.setconnect(rb.getString("source.canal.server"), Integer.parseInt(rb.getString("source.canal.port")), rb.getString("source.canal.instance"));
        int totalEmptyCount = Integer.parseInt(rb.getString("empty.second"));
        int emptyCount = 0;
        while (emptyCount < totalEmptyCount) {
            cc.tansferEntry();
            if(!cc.getIsExecute()){//如果有执行错误，则退出
                break;
            }
            if(cc.getIsEmpty()){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }else{
                emptyCount = 0;
            }
            System.out.println("empty count : " + (++emptyCount));
        }
        cc.disconnect();
        
        Logger log=Logger.getLogger(SyncClient.class);
        //程序结束发送邮件
        try{
            InetAddress addr = InetAddress.getLocalHost();
            String ip=addr.getHostAddress();
        
            MailHelper mh=new MailHelper(rb.getString("mail.server"),rb.getString("mail.user"),rb.getString("mail.password"));
            mh.sendEmail(rb.getString("mail.to"), ip + " synchronization", ip + " synchronization of has stoped.");
        }catch(UnknownHostException e){
            log.error(e.toString());
        }
    }
}
